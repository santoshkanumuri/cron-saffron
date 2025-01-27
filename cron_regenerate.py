# Filename: app.py
import pandas as pd
import pymongo
import dateutil.parser
import pinecone
from datetime import datetime
import os
import tempfile
import dotenv
from pinecone.grpc import PineconeGRPC as Pinecone
import logging
from typing import Dict, List, Optional, Tuple
from pymongo.errors import BulkWriteError

# --------------------- Configuration ---------------------
# Load environment variables from .env file
dotenv.load_dotenv()

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

# Pinecone Configuration
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = os.getenv("PINECONE_INDEX")

# Output Configuration
OUTPUT_CSV = "updated_matches.csv"

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('regenerate_matches.log'),
        logging.StreamHandler()
    ]
)

def validate_environment():
    """Validate required environment variables"""
    required_vars = {
        "MONGO_URI": MONGO_URI,
        "DB_NAME": DATABASE_NAME,
        "COLLECTION_NAME": COLLECTION_NAME,
        "PINECONE_API_KEY": PINECONE_API_KEY,
        "PINECONE_INDEX": INDEX_NAME
    }

    missing = [name for name, val in required_vars.items() if not val]
    if missing:
        error_msg = f"Missing environment variables: {', '.join(missing)}"
        logging.error(error_msg)
        raise EnvironmentError(error_msg)

def initialize_pinecone(api_key: str, index_name: str):
    """Initialize Pinecone connection and return index handle"""
    try:
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)
        logging.info(f"Connected to Pinecone index '{index_name}'")
        return index
    except Exception as e:
        logging.error(f"Pinecone connection error: {str(e)}")
        raise

def connect_mongodb(uri: str, db_name: str, collection_name: str):
    """Establish MongoDB connection and return collection handle"""
    try:
        client = pymongo.MongoClient(uri)
        db = client[db_name]
        collection = db[collection_name]
        logging.info(f"Connected to MongoDB collection '{collection_name}'")
        return collection
    except Exception as e:
        logging.error(f"MongoDB connection error: {str(e)}")
        raise

def fetch_documents(collection):
    """Retrieve documents containing 'none_@file' field"""
    try:
        query = {"none_@file": {"$exists": True, "$ne": None}}
        projection = {"none_@file": 1, "winning_bid": 1, "iso_date": 1, "auction_house": 1}
        documents = list(collection.find(query, projection))
        logging.info(f"Fetched {len(documents)} documents from MongoDB")
        return documents
    except Exception as e:
        logging.error(f"Document fetch error: {str(e)}")
        raise

def build_hashmaps(documents):
    """Create lookup dictionaries for fast data access"""
    image_id_to_winning_bid = {}
    image_id_to_date = {}
    image_id_to_auction_house = {}

    for doc in documents:
        raw_image_id = doc.get('none_@file', '')
        if not raw_image_id:
            continue

        try:
            image_id = raw_image_id.replace('\\', '/').strip()
            image_id_to_winning_bid[image_id] = doc.get('winning_bid')

            iso_date = doc.get('iso_date')
            image_id_to_date[image_id] = iso_date.isoformat() if iso_date else None

            image_id_to_auction_house[image_id] = doc.get('auction_house')
        except Exception as e:
            logging.warning(f"Error processing document {doc.get('_id')}: {str(e)}")
            continue

    logging.info("Hashmaps built successfully")
    return image_id_to_winning_bid, image_id_to_date, image_id_to_auction_house

def query_pinecone(index: Pinecone, image_id: str, top_k: int = 30) -> List[Dict]:
    """Query Pinecone vector database for similar images"""
    if not image_id or pd.isna(image_id):
        return []

    try:
        query_result = index.query(id=image_id, top_k=top_k, include_values=False)
        return query_result.get('matches', [])
    except Exception as e:
        logging.error(f"Pinecone query error for {image_id}: {str(e)}")
        return []

def process_matches(
    doc,
    matches,
    image_id_to_winning_bid,
    image_id_to_date,
    image_id_to_auction_house
) :
    """Process and categorize Pinecone matches into different groups"""
    # Initialize document fields
    for category in ['overall', 'same_day', 'before']:
        for i in range(1, 6):
            doc[f"{category}_match_{i}_id"] = None
            doc[f"{category}_match_{i}_price"] = None
            doc[f"{category}_match_{i}_score"] = None

    raw_image_id = doc.get('none_@file', '')
    if not raw_image_id:
        return doc

    image_id = raw_image_id.replace('\\', '/').strip()
    input_date_str = image_id_to_date.get(image_id)
    input_auction_house = image_id_to_auction_house.get(image_id)

    try:
        input_date = dateutil.parser.parse(input_date_str) if input_date_str else None
    except Exception as e:
        logging.warning(f"Date parsing error for {image_id}: {str(e)}")
        input_date = None

    # Initialize match containers
    categories = {
        'overall': [],
        'same_day': [],
        'before': []
    }

    for match in matches:
        if len(categories['overall']) >=5 and len(categories['same_day']) >=5 and len(categories['before']) >=5:
            break

        match_id = match['id'].replace('\\', '/')
        match_score = round(match['score'] * 100, 2)
        match_price = image_id_to_winning_bid.get(match_id)
        match_date_str = image_id_to_date.get(match_id)
        match_auction_house = image_id_to_auction_house.get(match_id)

        try:
            match_date = dateutil.parser.parse(match_date_str) if match_date_str else None
        except Exception as e:
            logging.debug(f"Match date parsing error for {match_id}: {str(e)}")
            continue

        match_data = {
            'id': match_id,
            'price': match_price,
            'score': match_score
        }

        # Categorization logic
        if input_date and match_date:
            if match_date.date() == input_date.date() and input_auction_house == match_auction_house:
                if len(categories['same_day']) < 5:
                    categories['same_day'].append(match_data)
            elif match_date < input_date:
                if len(categories['before']) < 5:
                    categories['before'].append(match_data)

        if len(categories['overall']) < 5:
            categories['overall'].append(match_data)

    # Populate document fields
    for category, matches in categories.items():
        for idx, match in enumerate(matches[:5], start=1):
            doc[f"{category}_match_{idx}_id"] = match['id']
            doc[f"{category}_match_{idx}_price"] = match['price']
            doc[f"{category}_match_{idx}_score"] = match['score']

    return doc


def update_mongodb(collection, documents) -> None:
    """Perform bulk update of MongoDB documents"""
    operations = []
    update_fields = [
        (f"{cat}_match_{i}_{field}",)
        for cat in ['overall', 'same_day', 'before']
        for i in range(1,6)
        for field in ['id', 'price', 'score']
    ]

    for doc in documents:
        update = {"$set": {}}
        for field in update_fields:
            field_name = field[0]
            if field_name in doc:
                update["$set"][field_name] = doc[field_name]

        if update["$set"]:
            operations.append(pymongo.UpdateOne(
                {"_id": doc["_id"]},
                update
            ))

    if not operations:
        logging.warning("No update operations to perform")
        return

    try:
        result = collection.bulk_write(operations, ordered=False)
        logging.info(
            f"MongoDB update summary: "
            f"Matched={result.matched_count}, "
            f"Modified={result.modified_count}, "
            f"Errors={len(result.bulk_api_result.get('writeErrors', []))}"
        )
    except BulkWriteError as bwe:
        logging.error(f"Bulk write error: {str(bwe.details)}")
    except Exception as e:
        logging.error(f"Update error: {str(e)}")

def regenerate_matches():
    """Main workflow controller"""
    logging.info("Starting match regeneration process")

    try:
        validate_environment()
        pinecone_index = initialize_pinecone(PINECONE_API_KEY, INDEX_NAME)
        collection = connect_mongodb(MONGO_URI, DATABASE_NAME, COLLECTION_NAME)
        documents = fetch_documents(collection)

        if not documents:
            logging.warning("No documents found with 'none_@file' field")
            return

        hashmaps = build_hashmaps(documents)
        updated_docs = []

        for idx, doc in enumerate(documents, 1):
            image_id = doc.get('none_@file', '')
            if not image_id:
                logging.debug(f"Skipping document {doc['_id']} with empty image ID")
                updated_docs.append(doc)
                continue

            matches = query_pinecone(pinecone_index, image_id)
            if not matches:
                updated_docs.append(doc)
                continue

            processed_doc = process_matches(doc, matches, *hashmaps)
            updated_docs.append(processed_doc)

            if idx % 100 == 0:
                logging.info(f"Processed {idx}/{len(documents)} documents")

        update_mongodb(collection, updated_docs)
        logging.info("Match regeneration completed successfully")

    except Exception as e:
        logging.error(f"Critical failure: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    regenerate_matches()