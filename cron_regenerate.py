# Filename: app.py
import pandas as pd
import pymongo
import dateutil.parser
import pinecone
from datetime import datetime
import os
# import tempfile # No longer used
import dotenv
from pinecone.grpc import PineconeGRPC as Pinecone # Assuming gRPC client
import logging
from typing import Dict, List, Optional, Tuple
from pymongo.errors import BulkWriteError, ConnectionFailure

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

# Processing Configuration
BATCH_SIZE = 1000 # Number of documents to update in MongoDB at a time

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s', # Keep function name for context
    handlers=[
        logging.FileHandler('regenerate_matches.log', mode='w'), # Overwrite log each run
        logging.StreamHandler()
    ]
)

# --------------------- Core Functions ---------------------

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
    logging.info("Environment variables validated.")

def initialize_pinecone(api_key: str, index_name: str):
    """Initialize Pinecone connection and return index handle"""
    try:
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)
        # Optional: Add a quick check to confirm connection
        index.describe_index_stats()
        logging.info(f"Successfully connected to Pinecone index '{index_name}'")
        return index
    except Exception as e:
        logging.error(f"Pinecone connection error: {str(e)}")
        raise

def connect_mongodb(uri: str, db_name: str, collection_name: str):
    """Establish MongoDB connection and return collection handle"""
    try:
        # Add reasonable timeouts
        client = pymongo.MongoClient(
            uri,
            serverSelectionTimeoutMS=10000, # 10 seconds to find server
            socketTimeoutMS=30000 # 30 seconds for operations
            )
        # Verify connection
        client.admin.command('ismaster')
        db = client[db_name]
        collection = db[collection_name]
        logging.info(f"Connected to MongoDB collection '{db_name}.{collection_name}'")
        return collection
    except ConnectionFailure as e:
        logging.error(f"MongoDB connection failed: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"MongoDB setup error: {str(e)}")
        raise

def fetch_documents(collection):
    """Retrieve documents containing 'none_@file' field, ensuring _id is included."""
    try:
        # Ensure field exists and is not None or empty string
        query = {"none_@file": {"$exists": True, "$ne": None, "$ne": ""}}
        # Project necessary fields, _id is included by default but good to be explicit
        projection = {"_id": 1, "none_@file": 1, "winning_bid": 1, "iso_date": 1, "auction_house": 1}
        documents = list(collection.find(query, projection))
        logging.info(f"Fetched {len(documents)} documents with non-empty 'none_@file' from MongoDB.")
        return documents
    except Exception as e:
        logging.error(f"Document fetch error: {str(e)}")
        raise

def build_hashmaps(documents):
    """Create lookup dictionaries for fast data access. Handles non-string image IDs."""
    image_id_to_winning_bid = {}
    image_id_to_date = {}
    image_id_to_auction_house = {}
    skipped_non_string = 0

    for doc in documents:
        raw_image_id = doc.get('none_@file') # Keep it simple, check type below

        # --- Essential Fix: Check type ---
        if not isinstance(raw_image_id, str):
            if skipped_non_string == 0: # Log first instance
                 logging.warning(f"Doc {doc.get('_id')}: 'none_@file' is not a string (type: {type(raw_image_id)}). Skipping this and subsequent non-string IDs.")
            skipped_non_string += 1
            continue
        # --- End Fix ---

        # Process if it's a valid string
        try:
            image_id = raw_image_id.replace('\\', '/').strip()
            if not image_id: # Skip if processing resulted in empty string
                 logging.debug(f"Doc {doc.get('_id')}: Processed image_id is empty. Skipping.")
                 continue

            image_id_to_winning_bid[image_id] = doc.get('winning_bid')

            iso_date = doc.get('iso_date')
            # Store date string directly if valid, None otherwise
            if isinstance(iso_date, datetime):
                image_id_to_date[image_id] = iso_date.isoformat()
            else:
                image_id_to_date[image_id] = None # Keep it simple

            image_id_to_auction_house[image_id] = doc.get('auction_house')

        except Exception as e:
            # Catch other errors during processing of this specific doc
            logging.warning(f"Error processing fields for document {doc.get('_id')} (Image ID: '{raw_image_id}'): {str(e)}")
            continue # Skip to next document

    if skipped_non_string > 0:
         logging.warning(f"Finished building hashmaps. Skipped {skipped_non_string} documents due to non-string 'none_@file'.")
    else:
         logging.info("Hashmaps built successfully.")

    return image_id_to_winning_bid, image_id_to_date, image_id_to_auction_house

def query_pinecone(index: Pinecone, image_id: str, top_k: int = 30) -> List[Dict]:
    """Query Pinecone vector database for similar images"""
    # Basic validation
    if not image_id: # pd.isna check removed as we ensure string in build_hashmaps
        return []

    try:
        # Assuming include_values=False is desired as per original
        query_result = index.query(id=image_id, top_k=top_k, include_values=False)
        # Return the list of match objects directly (assuming they are dict-like)
        matches = query_result.get('matches', [])
        if not isinstance(matches, list):
             logging.error(f"Pinecone query for {image_id} returned non-list 'matches': {type(matches)}")
             return []
        return matches
    except Exception as e:
        # Log specific common errors differently if needed
        if "vector id not found" in str(e).lower():
            logging.warning(f"Pinecone query skipped: Vector ID '{image_id}' not found.")
        else:
            logging.error(f"Pinecone query error for '{image_id}': {str(e)}")
        return [] # Return empty list on error

def process_matches(
    doc: Dict, # Use original doc reference
    matches: List, # Keep generic List type hint
    image_id_to_winning_bid: Dict,
    image_id_to_date: Dict,
    image_id_to_auction_house: Dict
) -> Dict: # Return the modified doc
    """
    Process and categorize Pinecone matches into different groups.
    Modifies the input doc dictionary directly.
    """
    # Initialize/clear fields in the document dictionary
    for category in ['overall', 'same_day', 'before']:
        for i in range(1, 6):
            doc[f"{category}_match_{i}_id"] = None
            doc[f"{category}_match_{i}_price"] = None
            doc[f"{category}_match_{i}_score"] = None

    # Assume raw_image_id is valid string based on build_hashmaps filter
    raw_image_id = doc.get('none_@file')
    if not isinstance(raw_image_id, str): # Double check just in case
         logging.error(f"Doc {doc.get('_id')}: process_matches called with non-string image_id - should not happen.")
         return doc # Return unmodified doc

    image_id = raw_image_id.replace('\\', '/').strip()
    if not image_id: return doc # Should not happen if build_hashmaps worked

    input_date_str = image_id_to_date.get(image_id)
    input_auction_house = image_id_to_auction_house.get(image_id)
    input_date = None

    if input_date_str:
        try:
            input_date = dateutil.parser.isoparse(input_date_str) # Prefer ISO format
        except ValueError:
            try:
                 input_date = dateutil.parser.parse(input_date_str) # Fallback
            except Exception as e:
                 logging.debug(f"Date parsing error for input {image_id}: {str(e)}")
                 # Keep input_date as None

    # Initialize match containers
    categories = {'overall': [], 'same_day': [], 'before': []}
    max_matches = 5

    # Use original direct access assuming match objects behave like dicts
    processed_match_ids = set()
    for match in matches:
        # Check if all categories are full
        if all(len(categories[cat]) >= max_matches for cat in categories):
            break

        try:
            # --- Direct Access as per Original ---
            raw_match_id = match['id']
            match_score_raw = match['score']
            # --- End Direct Access ---

            if not isinstance(raw_match_id, str):
                logging.warning(f"Doc {doc.get('_id')}: Skipping match with non-string id: {type(raw_match_id)}")
                continue

            match_id = raw_match_id.replace('\\', '/').strip()
            if not match_id: continue
            if match_id == image_id: continue # Skip self-match
            if match_id in processed_match_ids: continue # Avoid duplicates in response
            processed_match_ids.add(match_id)

            # Process score
            try:
                match_score = round(float(match_score_raw) * 100, 2)
            except (ValueError, TypeError) as score_err:
                logging.warning(f"Doc {doc.get('_id')}: Invalid score '{match_score_raw}' for match '{match_id}'. Error: {score_err}")
                continue # Skip match if score is invalid

            # Look up details only if match_id is known from our data
            if match_id not in image_id_to_date:
                # logging.debug(f"Match ID '{match_id}' not found in hashmaps.")
                match_data = {'id': match_id, 'price': None, 'score': match_score}
                # Still add to overall if space allows
                if len(categories['overall']) < max_matches:
                     categories['overall'].append(match_data)
                continue # Cannot categorize further

            # Match found in hashmaps, get details
            match_price = image_id_to_winning_bid.get(match_id)
            match_date_str = image_id_to_date.get(match_id)
            match_auction_house = image_id_to_auction_house.get(match_id)
            match_date = None

            if match_date_str:
                try: match_date = dateutil.parser.isoparse(match_date_str)
                except ValueError:
                    try: match_date = dateutil.parser.parse(match_date_str)
                    except Exception: pass # Ignore match date parse errors silently for now

            match_data = {'id': match_id, 'price': match_price, 'score': match_score}

            # Overall category (always add if space)
            if len(categories['overall']) < max_matches:
                categories['overall'].append(match_data)

            # Categorization logic
            if input_date and match_date:
                if len(categories['same_day']) < max_matches:
                    if match_date.date() == input_date.date() and input_auction_house == match_auction_house:
                         categories['same_day'].append(match_data)
                if len(categories['before']) < max_matches:
                    if match_date < input_date:
                         categories['before'].append(match_data)

        except KeyError as ke:
            # This will catch if 'id' or 'score' is missing using direct access
            logging.warning(f"Doc {doc.get('_id')}: Skipping match due to KeyError (likely missing 'id' or 'score'): {ke}. Match data: {str(match)[:100]}...")
            continue
        except Exception as e:
            # Catch unexpected errors during processing of a single match
            logging.error(f"Doc {doc.get('_id')}: Unexpected error processing match '{match_id if 'match_id' in locals() else 'unknown'}': {e}", exc_info=False) # Set exc_info=True for full traceback if needed
            continue

    # Populate document fields directly in the input 'doc'
    for category, collected_matches in categories.items():
        # Sort by score descending just in case Pinecone order changes
        sorted_matches = sorted(collected_matches, key=lambda x: x.get('score', 0.0), reverse=True)
        for idx, match_item in enumerate(sorted_matches[:max_matches], start=1):
            doc[f"{category}_match_{idx}_id"] = match_item['id']
            doc[f"{category}_match_{idx}_price"] = match_item['price']
            doc[f"{category}_match_{idx}_score"] = match_item['score']

    return doc # Return the modified document


def update_mongodb_batch(collection, batch_docs: List[Dict]):
    """Perform bulk update for a batch of documents. Uses $set only."""
    if not batch_docs:
        logging.debug("No documents in this batch to update.")
        return True # Considered success if nothing to do

    operations = []
    # Define fields to potentially update (as in original code)
    update_field_keys = [
        f"{cat}_match_{i}_{field}"
        for cat in ['overall', 'same_day', 'before']
        for i in range(1, 6)
        for field in ['id', 'price', 'score']
    ]

    for doc in batch_docs:
        doc_id = doc.get("_id")
        if not doc_id:
            logging.warning(f"Skipping update for doc missing '_id': {doc.get('none_@file')}")
            continue

        update_payload = {"$set": {}}
        has_updates = False
        for key in update_field_keys:
            # Check if key exists in the processed doc (process_matches adds them, possibly as None)
            if key in doc:
                 update_payload["$set"][key] = doc[key] # Set the value (even if None)
                 has_updates = True # Mark that there are changes

        # Only add operation if there's something to $set
        if has_updates:
            operations.append(pymongo.UpdateOne(
                {"_id": doc_id},
                update_payload # Use $set only, as per original logic implied
            ))

    if not operations:
        logging.info("No update operations generated for this batch.")
        return True

    logging.info(f"Attempting MongoDB bulk write for {len(operations)} operations in this batch...")
    try:
        result = collection.bulk_write(operations, ordered=False)
        logging.info(
            f"MongoDB batch update summary: "
            f"Matched={result.matched_count}, "
            f"Modified={result.modified_count}, "
            f"Errors={len(result.bulk_api_result.get('writeErrors', [])) if result.bulk_api_result else 'N/A'}"
        )
        # Check for write errors in the result
        if result.bulk_api_result and result.bulk_api_result.get('writeErrors'):
             logging.error(f"Batch update had {len(result.bulk_api_result['writeErrors'])} errors.")
             # Log first few errors
             for i, err in enumerate(result.bulk_api_result['writeErrors'][:3]):
                  logging.error(f"  - Batch Err {i+1}: Index {err.get('index', 'N/A')}, Code {err.get('code', 'N/A')}, Msg: {err.get('errmsg', 'N/A')}")
             return False # Indicate batch had issues
        return True # Batch successful or completed with non-fatal errors reported above

    except BulkWriteError as bwe:
        logging.error(f"MongoDB batch update failed: {bwe.details}", exc_info=True)
        return False # Indicate batch failure
    except ConnectionFailure as cfe:
         logging.error(f"MongoDB batch update failed due to connection error: {cfe}", exc_info=True)
         return False # Indicate batch failure
    except Exception as e:
        logging.error(f"Unexpected error during MongoDB batch update: {str(e)}", exc_info=True)
        return False # Indicate batch failure


def regenerate_matches():
    """Main workflow controller with batching"""
    logging.info("="*20 + " Starting Match Regeneration " + "="*20)
    start_time = datetime.now()
    total_processed = 0
    total_failed_batches = 0

    try:
        # --- Setup ---
        validate_environment()
        pinecone_index = initialize_pinecone(PINECONE_API_KEY, INDEX_NAME)
        collection = connect_mongodb(MONGO_URI, DATABASE_NAME, COLLECTION_NAME)

        # --- Fetch and Prepare ---
        documents = fetch_documents(collection)
        if not documents:
            logging.warning("No documents found with 'none_@file' field. Exiting.")
            return
        total_docs = len(documents)
        logging.info(f"Building hashmaps for {total_docs} documents...")
        hashmaps = build_hashmaps(documents)
        valid_ids = set(hashmaps[1].keys()) # Use date map keys as indicator of success
        logging.info(f"Processing {len(valid_ids)} documents with valid image IDs.")

        # --- Processing Loop with Batching ---
        batch_to_update = []
        for idx, doc in enumerate(documents, 1):
            total_processed += 1
            # Get the image ID safely after build_hashmaps validation
            raw_image_id = doc.get('none_@file')
            if not isinstance(raw_image_id, str): continue # Skip if missed by build_hashmaps somehow
            image_id = raw_image_id.replace('\\', '/').strip()
            if not image_id or image_id not in valid_ids:
                # This doc had an issue during hashmap build, skip Pinecone query
                # Process it to ensure fields are cleared if necessary
                processed_doc = process_matches(doc, [], *hashmaps) # Pass empty matches list
                batch_to_update.append(processed_doc) # Add for potential field clearing update
            else:
                # Valid image ID, query Pinecone
                matches = query_pinecone(pinecone_index, image_id)
                processed_doc = process_matches(doc, matches, *hashmaps)
                batch_to_update.append(processed_doc)

            # Log progress periodically
            if idx % (BATCH_SIZE * 2) == 0: # Log less frequently than batch writes
                logging.info(f"--- Progress: Processed {idx}/{total_docs} documents ---")

            # Check if batch is full or it's the last document
            if len(batch_to_update) >= BATCH_SIZE or idx == total_docs:
                batch_num = (idx + BATCH_SIZE - 1) // BATCH_SIZE
                logging.info(f"--- Submitting Batch {batch_num} ({len(batch_to_update)} docs) to MongoDB ---")
                success = update_mongodb_batch(collection, batch_to_update)
                if not success:
                    total_failed_batches += 1
                    logging.error(f"--- Batch {batch_num} FAILED to update in MongoDB. Check logs. ---")
                else:
                     logging.info(f"--- Batch {batch_num} update attempted. ---")

                batch_to_update = [] # Clear batch

        # --- Completion ---
        end_time = datetime.now()
        logging.info("="*20 + " Regeneration Summary " + "="*20)
        logging.info(f"Finished processing {total_processed}/{total_docs} documents.")
        logging.info(f"Total failed MongoDB update batches: {total_failed_batches}")
        logging.info(f"Total execution time: {end_time - start_time}")
        if total_failed_batches > 0:
             logging.warning("Check log file for details on failed MongoDB batches.")
        logging.info("Match regeneration completed.")

    except EnvironmentError as e:
        logging.critical(f"Configuration error: {e}")
    except ConnectionFailure as e:
        logging.critical(f"Initial MongoDB connection failed: {e}")
    except Exception as e:
        logging.critical(f"Critical unexpected error during regeneration: {e}", exc_info=True)

if __name__ == "__main__":
    regenerate_matches()
