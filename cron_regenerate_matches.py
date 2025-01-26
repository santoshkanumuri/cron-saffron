# Filename: app.py
import pandas as pd
from pymongo import MongoClient
import dateutil.parser
import pinecone
from datetime import datetime
import os
import tempfile
import dotenv
from pinecone.grpc import PineconeGRPC as Pinecone
import logging


logging.basicConfig(
    filename='regenerate_matches.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# Load environment variables from .env file
dotenv.load_dotenv()
# --------------------- Configuration ---------------------

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")  # Replace with your MongoDB URI
DATABASE_NAME = os.getenv("DB_NAME")  # Replace with your MongoDB database name
COLLECTION_NAME = os.getenv("COLLECTION_NAME")  # Replace with your MongoDB collection name

# Pinecone Configuration
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")  # Replace with your Pinecone API key
INDEX_NAME = os.getenv("PINECONE_INDEX")  # Replace with your Pinecone index name

# Output Configuration
OUTPUT_CSV = "updated_matches.csv"

# ----------------------------------------------------------

def initialize_pinecone(api_key, index_name):
    pc=Pinecone(api_key=api_key)
    try:
        index = pc.Index(index_name)
        logging.info(f"Connected to Pinecone index '{index_name}'.")
        logging.info(f"Connected to Pinecone index '{index_name}'.")
        return index
    except Exception as e:
        logging.error(f"Error connecting to Pinecone index '{index_name}': {e}")
        logging.error(f"Pinecone connection error: {e}")
        raise Exception(f"Error initializing Pinecone index: {e}")

def connect_mongodb(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

def fetch_documents(collection):
    # Fetch documents where 'none_@file' exists
    documents = list(collection.find({"none_@file": {"$exists": True}}))
    return documents

def build_hashmaps(documents):
    image_id_to_winning_bid = {}
    image_id_to_date = {}
    image_id_to_auction_house = {}
    for doc in documents:
        image_id = doc.get('none_@file')
        if image_id:
            image_id_to_winning_bid[image_id] = doc.get('winning_bid', None)
            image_id_to_date[image_id] = doc.get('iso_date', None)
            image_id_to_auction_house[image_id] = doc.get('auction_house', None)
    return image_id_to_winning_bid, image_id_to_date, image_id_to_auction_house

def query_pinecone(index, image_id, top_k=30):
    try:
        if not pd.isna(image_id):
            query_result = index.query(id=image_id, top_k=top_k, include_values=False)
            logging.info(f"Query result for image {image_id}: len={len(query_result['matches'])}")
            print(f"Query result for image {image_id}: len={len(query_result['matches'])}")
            if not query_result['matches']:
                return []
            matches = query_result['matches'][0].get('matches', [])
            print(f"Matches for image {image_id}: {matches}")
            return matches
    except Exception as e:
        logging.error(f"Error querying Pinecone for image {image_id}: {e}")
        print(f"Error querying Pinecone for image {image_id}: {e}")
        return []

def process_matches(doc, matches, image_id_to_winning_bid, image_id_to_date, image_id_to_auction_house):
    image_id = doc['none_@file']
    input_date_str = image_id_to_date.get(image_id)
    input_auction_house = image_id_to_auction_house.get(image_id)

    try:
        input_date = dateutil.parser.parse(input_date_str)
    except Exception as e:
        logging.warning(f"Error parsing date for image {image_id}: {e}")
        input_date = None

    overall_matches = []
    same_day_matches = []
    before_matches = []

    for match in matches:
        match_id = match['id'].replace('\\', '/')
        match_score = round(match['score'] * 100, 2)
        match_price = image_id_to_winning_bid.get(match_id, None)
        match_date_str = image_id_to_date.get(match_id, None)
        match_auction_house = image_id_to_auction_house.get(match_id, None)

        # Parse match date
        try:
            match_date = dateutil.parser.parse(match_date_str)
        except Exception as e:
            logging.warning(f"Error parsing date for match {match_id}: {e}")
            continue

        # Categorize matches
        if input_date and match_date == input_date and input_auction_house == match_auction_house:
            if len(same_day_matches) < 5:
                same_day_matches.append({
                    'id': match_id,
                    'price': match_price,
                    'score': match_score
                })
        elif input_date and match_date < input_date:
            if len(before_matches) < 5:
                before_matches.append({
                    'id': match_id,
                    'price': match_price,
                    'score': match_score
                })
        if len(overall_matches) < 5:
            overall_matches.append({
                'id': match_id,
                'price': match_price,
                'score': match_score
            })

        # Break if all categories have 5 matches
        if len(overall_matches) >= 5 and len(same_day_matches) >= 5 and len(before_matches) >= 5:
            break

    # Fill the matches into the document
    for i in range(5):
        # Overall Matches
        if i < len(overall_matches):
            doc[f'overall_match_{i+1}_id'] = overall_matches[i]['id']
            doc[f'overall_match_{i+1}_price'] = overall_matches[i]['price']
            doc[f'overall_match_{i+1}_score'] = overall_matches[i]['score']
        else:
            doc[f'overall_match_{i+1}_id'] = None
            doc[f'overall_match_{i+1}_price'] = None
            doc[f'overall_match_{i+1}_score'] = None

        # Same Day Matches
        if i < len(same_day_matches):
            doc[f'same_day_match_{i+1}_id'] = same_day_matches[i]['id']
            doc[f'same_day_match_{i+1}_price'] = same_day_matches[i]['price']
            doc[f'same_day_match_{i+1}_score'] = same_day_matches[i]['score']
        else:
            doc[f'same_day_match_{i+1}_id'] = None
            doc[f'same_day_match_{i+1}_price'] = None
            doc[f'same_day_match_{i+1}_score'] = None

        # Before Matches
        if i < len(before_matches):
            doc[f'before_match_{i+1}_id'] = before_matches[i]['id']
            doc[f'before_match_{i+1}_price'] = before_matches[i]['price']
            doc[f'before_match_{i+1}_score'] = before_matches[i]['score']
        else:
            doc[f'before_match_{i+1}_id'] = None
            doc[f'before_match_{i+1}_price'] = None
            doc[f'before_match_{i+1}_score'] = None

    return doc

def regenerate_matches():
    logging.info("Starting regeneration process...")
    try:
        pinecone_index = initialize_pinecone(PINECONE_API_KEY, INDEX_NAME)
        logging.info("Pinecone initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing Pinecone: {e}")
        return
    try:
        collection = connect_mongodb(MONGO_URI, DATABASE_NAME, COLLECTION_NAME)
        logging.info("Connected to MongoDB successfully.")
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        return

    try:
        documents = fetch_documents(collection)
        logging.info(f"Fetched {len(documents)} documents from MongoDB.")
    except Exception as e:
        logging.error(f"Error fetching documents: {e}")
        return

    if not documents:
        logging.warning("No documents found with 'none_@file' field.")
        return


    image_id_to_winning_bid, image_id_to_date, image_id_to_auction_house = build_hashmaps(documents)
    logging.info("Hashmaps built successfully.")

    updated_documents = []
    total_docs = len(documents)


    for idx, doc in enumerate(documents, 1):
        image_id = doc.get('none_@file')
        if not image_id:
            logging.info(f"Document {idx}/{total_docs} missing 'none_@file'. Skipping.")
            print(f"Document {idx}/{total_docs} missing 'none_@file'. Skipping.")
            updated_documents.append(doc)
            continue

        logging.info(f"Processing document {idx}/{total_docs} with none_@file: {image_id}")
        print(f"Processing document {idx}/{total_docs} with none_@file: {image_id}")

        # Query Pinecone
        matches = query_pinecone(pinecone_index, image_id, top_k=30)

        if not matches:
            logging.info(f"No matches found for image {image_id}.")
            updated_documents.append(doc)
            continue

        # Process matches and categorize
        updated_doc = process_matches(doc, matches, image_id_to_winning_bid, image_id_to_date, image_id_to_auction_house)
        updated_documents.append(updated_doc)
        progress_bar.progress(idx / total_docs)

    logging.info("Updating documents in MongoDB...")
    try:
        for doc in updated_documents:
            update_fields = {
                # Overall Matches
                "overall_match_1_id": doc.get("overall_match_1_id"),
                "overall_match_1_price": doc.get("overall_match_1_price"),
                "overall_match_1_score": doc.get("overall_match_1_score"),
                "overall_match_2_id": doc.get("overall_match_2_id"),
                "overall_match_2_price": doc.get("overall_match_2_price"),
                "overall_match_2_score": doc.get("overall_match_2_score"),
                "overall_match_3_id": doc.get("overall_match_3_id"),
                "overall_match_3_price": doc.get("overall_match_3_price"),
                "overall_match_3_score": doc.get("overall_match_3_score"),
                "overall_match_4_id": doc.get("overall_match_4_id"),
                "overall_match_4_price": doc.get("overall_match_4_price"),
                "overall_match_4_score": doc.get("overall_match_4_score"),
                "overall_match_5_id": doc.get("overall_match_5_id"),
                "overall_match_5_price": doc.get("overall_match_5_price"),
                "overall_match_5_score": doc.get("overall_match_5_score"),

                # Same Day Matches
                "same_day_match_1_id": doc.get("same_day_match_1_id"),
                "same_day_match_1_price": doc.get("same_day_match_1_price"),
                "same_day_match_1_score": doc.get("same_day_match_1_score"),
                "same_day_match_2_id": doc.get("same_day_match_2_id"),
                "same_day_match_2_price": doc.get("same_day_match_2_price"),
                "same_day_match_2_score": doc.get("same_day_match_2_score"),
                "same_day_match_3_id": doc.get("same_day_match_3_id"),
                "same_day_match_3_price": doc.get("same_day_match_3_price"),
                "same_day_match_3_score": doc.get("same_day_match_3_score"),
                "same_day_match_4_id": doc.get("same_day_match_4_id"),
                "same_day_match_4_price": doc.get("same_day_match_4_price"),
                "same_day_match_4_score": doc.get("same_day_match_4_score"),
                "same_day_match_5_id": doc.get("same_day_match_5_id"),
                "same_day_match_5_price": doc.get("same_day_match_5_price"),
                "same_day_match_5_score": doc.get("same_day_match_5_score"),

                # Before Matches
                "before_match_1_id": doc.get("before_match_1_id"),
                "before_match_1_price": doc.get("before_match_1_price"),
                "before_match_1_score": doc.get("before_match_1_score"),
                "before_match_2_id": doc.get("before_match_2_id"),
                "before_match_2_price": doc.get("before_match_2_price"),
                "before_match_2_score": doc.get("before_match_2_score"),
                "before_match_3_id": doc.get("before_match_3_id"),
                "before_match_3_price": doc.get("before_match_3_price"),
                "before_match_3_score": doc.get("before_match_3_score"),
                "before_match_4_id": doc.get("before_match_4_id"),
                "before_match_4_price": doc.get("before_match_4_price"),
                "before_match_4_score": doc.get("before_match_4_score"),
                "before_match_5_id": doc.get("before_match_5_id"),
                "before_match_5_price": doc.get("before_match_5_price"),
                "before_match_5_score": doc.get("before_match_5_score"),
            }

            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": update_fields}
            )
        logging.info("All documents updated successfully in MongoDB.")
    except Exception as e:
        logging.error(f"Error updating documents in MongoDB: {e}")
        return


if __name__ == "__main__":
    regenerate_matches()
