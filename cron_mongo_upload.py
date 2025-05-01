# Filename: mongo_upload.py (Assuming this is the file name)
import pandas as pd
import numpy as np
import os
import sys
import chardet
import logging
import requests
import boto3
import pinecone
import tensorflow as tf
import tensorflow_hub as hub
from PIL import Image, UnidentifiedImageError
# NOTE: tf_keras is unusual, typically it's just `from tensorflow import keras`
# If tf_keras is a specific library you installed, keep it. Otherwise, consider using standard tensorflow.keras
import tf_keras
from pymongo import MongoClient, UpdateOne # Import UpdateOne for later use maybe
from pymongo.errors import BulkWriteError # Import if using bulk writes
from dotenv import load_dotenv
from io import BytesIO
from urllib.parse import urlparse
# Import ClientError for S3 error handling
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from requests.exceptions import RequestException
from pinecone.grpc import PineconeGRPC as Pinecone

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='mongo_upload.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# Set up TensorFlow model
# Consider making MODEL_URL an environment variable if it might change
MODEL_URL = 'https://tfhub.dev/google/tf2-preview/mobilenet_v2/classification/4'
# Wrap model loading in try-except or ensure network access if loading from URL at runtime
try:
    MODEL = tf_keras.Sequential([hub.KerasLayer(MODEL_URL)])
    logging.info(f"TensorFlow Hub model loaded from {MODEL_URL}")
except Exception as e:
    logging.critical(f"Failed to load TensorFlow Hub model: {e}", exc_info=True)
    # Depending on requirements, you might want to exit here
    sys.exit("Model loading failed. Cannot proceed.")

# --- Functions (read_csv_from_s3, read_uploaded_file, etc. remain unchanged) ---

# Assume these functions exist as you provided them:
# def read_csv_from_s3(...): ...
# def read_uploaded_file(...): ...
# def detect_file_type(...): ...
# def read_csv_file(...): ...
# def validate_data(...): ...
# def extract_from_image_url(...): ... # (This uses MODEL, ensure it's loaded)
# def upload_to_mongodb(...): ...

# ----- Functions from the original snippet -----
def read_csv_from_s3(bucket_name, object_name, local_path=None, **kwargs):
    """
    Read a CSV file from an S3 bucket and optionally save it locally

    :param bucket_name: Name of the S3 bucket
    :param object_name: S3 object key/path to the CSV file
    :param local_path: (Optional) Local path to save the CSV file
    :param kwargs: Additional arguments for pandas.read_csv()
    :return: pandas.DataFrame or None if unsuccessful
    """
    # Uses default boto3 session which will pick up IAM role
    s3_client = boto3.client('s3')

    try:
        # Get the CSV object from S3
        logging.info(f"Attempting to get object s3://{bucket_name}/{object_name}")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        logging.info(f"Successfully retrieved object metadata.")

        # Read CSV directly into DataFrame
        # Detect encoding first for robustness
        logging.info("Detecting CSV encoding...")
        try:
            raw_body = response['Body'].read()
            detected_encoding = chardet.detect(raw_body)['encoding']
            if not detected_encoding:
                 detected_encoding = 'utf-8' # Default fallback
            logging.info(f"Detected encoding: {detected_encoding}. Reading CSV...")
            # Use BytesIO to treat the read bytes as a file for pandas
            df = pd.read_csv(BytesIO(raw_body), encoding=detected_encoding, **kwargs)
        except UnicodeDecodeError:
            logging.warning(f"Failed with detected encoding {detected_encoding}. Falling back to utf-8.")
            df = pd.read_csv(BytesIO(raw_body), encoding='utf-8', **kwargs) # Try utf-8
        except Exception as read_err:
            logging.error(f"Error reading CSV content: {read_err}", exc_info=True)
            raise # Re-raise to be caught by outer try-except

        # Optionally save to local file (Re-fetch or use the read bytes)
        if local_path:
            try:
                # Use the raw_body we already read
                with open(local_path, 'wb') as f:
                    f.write(raw_body)
                logging.info(f"File also saved locally to {local_path}")
            except Exception as save_err:
                 logging.error(f"Could not save file locally to {local_path}: {save_err}")


        logging.info(f"Successfully read CSV from s3://{bucket_name}/{object_name}")
        return df

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logging.error(f"Error: File '{object_name}' not found in bucket '{bucket_name}'")
        elif error_code == 'NoSuchBucket':
            logging.error(f"Error: Bucket '{bucket_name}' does not exist")
        else:
            logging.error(f"AWS Client Error accessing s3://{bucket_name}/{object_name}: {str(e)}")
        return None
    except NoCredentialsError:
        logging.error("Error: AWS credentials not found (and IAM role might be missing/misconfigured)")
        return None
    except pd.errors.EmptyDataError:
         logging.error(f"CSV Parsing Error: File s3://{bucket_name}/{object_name} is empty.")
         return None
    except pd.errors.ParserError as e:
        logging.error(f"CSV Parsing Error for s3://{bucket_name}/{object_name}: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Unexpected Error reading S3 CSV s3://{bucket_name}/{object_name}: {str(e)}", exc_info=True)
        return None

def validate_data(df):
    """
    Validates the DataFrame to ensure it contains required columns.
    """
    required_columns = [
        'lot_id', 'none_@file', 'image_url', 'lot_link',
        'iso_date', 'lo_est', 'hi_est', 'winning_bid',
        'auction_link', 'est_curr'
        # Add any other columns absolutely essential for processing
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Validation Error: Missing required columns in input data: {', '.join(missing_columns)}")
        return False

    # Optional: Add more validation (e.g., check data types, check for empty essential columns)
    if df['none_@file'].isnull().any():
        logging.error("Validation Error: Found rows with missing 'none_@file'. This is required as an ID.")
        # Optionally filter out bad rows or return False
        # df.dropna(subset=['none_@file'], inplace=True) # Example: remove rows
        return False # Fail validation if critical ID is missing

    logging.info("Data validation passed.")
    return True

def process_images_and_embeddings(data):
    """
    Processes images by generating embeddings, uploading images to S3, and embeddings to Pinecone.
    Relies on IAM role for S3 credentials.
    """
    # Get S3 Bucket name from environment variables
    BUCKET_NAME = os.getenv('BUCKET_NAME')

    # Get Pinecone API key and environment from environment variables
    PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
    PINECONE_INDEX = os.getenv('PINECONE_INDEX')

    # Validate required environment variables
    if not BUCKET_NAME: # Only check for bucket name now
        logging.error("AWS S3 bucket name (BUCKET_NAME) not set in environment variables.")
        return # Cannot proceed without bucket name

    if not all([PINECONE_API_KEY, PINECONE_INDEX]):
        logging.error("Pinecone credentials (PINECONE_API_KEY, PINECONE_INDEX) not set in environment variables.")
        return

    # Initialize AWS S3 client - REMOVED EXPLICIT CREDENTIALS
    # Boto3 will automatically use the IAM role attached to the EC2 instance
    try:
        s3 = boto3.client('s3')
        # Optional: Verify access by listing buckets (requires ListBuckets permission) or checking bucket existence
        s3.head_bucket(Bucket=BUCKET_NAME) # Throws exception if bucket doesn't exist or no permission
        logging.info(f"Verified access to S3 bucket '{BUCKET_NAME}' using IAM role.")
    except ClientError as e:
         logging.error(f"S3 Error verifying bucket '{BUCKET_NAME}' with IAM role: {e}. Check role permissions (HeadBucket).")
         return
    except NoCredentialsError:
        logging.error("AWS credentials/IAM role not found by boto3.")
        return
    except Exception as e:
        logging.error(f"Unexpected error initializing S3 client or verifying bucket: {e}")
        return


    # Initialize Pinecone
    try:
        pc=Pinecone(api_key=PINECONE_API_KEY)
        index = pc.Index(PINECONE_INDEX)
        index.describe_index_stats() # Verify connection
        logging.info(f"Connected to Pinecone index '{PINECONE_INDEX}'.")
    except Exception as e:
        logging.error(f"Pinecone connection error: {e}", exc_info=True)
        return

    total_records = len(data)
    processed_count = 0
    skipped_count = 0
    s3_upload_errors = 0
    pinecone_upload_errors = 0

    for idx, record in enumerate(data):
        # Logging progress more efficiently
        if (idx + 1) % 100 == 0 or idx == 0 or idx == total_records - 1:
             progress = (idx + 1) / total_records
             logging.info(f"Processing record {idx + 1}/{total_records} ({progress:.1%})")

        image_url = record.get('image_url')
        # Use 'none_@file' as the primary identifier
        record_id = record.get('none_@file') # Use a clearer variable name

        if not image_url or not record_id:
            logging.warning(f"Record {idx+1} missing 'image_url' or 'none_@file'. Skipping.")
            skipped_count += 1
            continue

        # Clean the ID for S3 and Pinecone (replace backslashes)
        # Do this once
        pinecone_id = str(record_id).replace("\\", "/") # Ensure it's a string for Pinecone
        s3_key = pinecone_id # Use the same cleaned ID for S3 key

        try:
            # 1. Generate embeddings
            # Pass record_id for better error logging inside the function if needed
            embeddings = extract_from_image_url(image_url, record_id)
            if embeddings is None:
                # Error already logged in extract_from_image_url
                skipped_count += 1
                continue
            # Basic check on embedding dimension (adjust '1001' if your model output is different)
            if embeddings.ndim != 1 or embeddings.shape[0] != 1001:
                 logging.warning(f"Record {idx+1} (ID: {record_id}): Unexpected embedding shape {embeddings.shape}. Skipping.")
                 skipped_count += 1
                 continue

            # 2. Upsert embeddings to Pinecone
            try:
                # Pinecone expects list of floats
                index.upsert([(pinecone_id, embeddings.tolist())])
                # logging.debug(f"Upserted embeddings to Pinecone for id: {pinecone_id}") # Use debug level
            except Exception as pinecone_err:
                logging.error(f"Record {idx+1} (ID: {record_id}): Failed to upsert embeddings to Pinecone: {pinecone_err}")
                pinecone_upload_errors += 1
                # Decide if you want to continue processing this record (e.g., still upload to S3)
                # continue # Or skip the rest for this record

            # 3. Download image for S3 upload
            try:
                response = requests.get(image_url, stream=True, timeout=20) # Increased timeout
                response.raise_for_status() # Check for HTTP errors (4xx, 5xx)
                image_data = response.content
                if not image_data:
                     logging.warning(f"Record {idx+1} (ID: {record_id}): Downloaded empty image data from {image_url}. Skipping S3 upload.")
                     skipped_count += 1
                     continue
            except RequestException as download_err:
                logging.error(f"Record {idx+1} (ID: {record_id}): Error downloading image from URL {image_url}: {download_err}")
                skipped_count += 1
                continue # Skip S3 upload if download fails

            # 4. Upload image to S3
            try:
                s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=image_data)
                # logging.debug(f"Uploaded image to S3 bucket '{BUCKET_NAME}' with key '{s3_key}'") # Use debug level

                # 5. Add 's3_url' to record *only after successful upload*
                s3_url_prefix = f'https://{BUCKET_NAME}.s3.amazonaws.com/'
                record['s3_url'] = s3_url_prefix + s3_key
                processed_count += 1

            except ClientError as s3_err:
                 logging.error(f"Record {idx+1} (ID: {record_id}): Failed to upload image to S3 (Key: {s3_key}): {s3_err}")
                 s3_upload_errors += 1
                 # Decide if you want to clear the s3_url if upload failed
                 if 's3_url' in record: del record['s3_url']
                 continue # Or potentially stop if S3 upload is critical

        except Exception as e:
            # Catch-all for unexpected errors during processing a single record
            logging.error(f"Record {idx+1} (ID: {record_id}): Unexpected error during processing loop: {e}", exc_info=True)
            skipped_count += 1
            continue

    logging.info(f"Image processing completed. Processed: {processed_count}, Skipped: {skipped_count}, S3 Errors: {s3_upload_errors}, Pinecone Errors: {pinecone_upload_errors}")


def extract_from_image_url(url, record_id_for_logging="N/A"):
    """
    Extracts image embeddings using TensorFlow Hub model.
    Includes record ID in logs for better context.
    """
    try:
        # logging.debug(f"Record ID {record_id_for_logging}: Processing image URL: {url}")

        # Download image
        response = requests.get(url, stream=True, timeout=20) # Consistent timeout
        response.raise_for_status()
        image_data = response.content
        if not image_data:
             logging.warning(f"Record ID {record_id_for_logging}: Downloaded empty image data from {url}")
             return None

        # Open the image using PIL
        # Add error handling for corrupt image data
        try:
             image = Image.open(BytesIO(image_data)).convert('RGB')
        except UnidentifiedImageError as image_err:
             logging.error(f"Record ID {record_id_for_logging}: PIL UnidentifiedImageError for URL {url}: {image_err}")
             return None
        except Exception as pil_err:
             logging.error(f"Record ID {record_id_for_logging}: PIL Error opening image from URL {url}: {pil_err}")
             return None


        # Resize the image to the desired shape (e.g., 224x224 for MobileNet V2)
        # Make target size configurable or constant
        target_size = (224, 224)
        image = image.resize(target_size)

        # Convert the image to a NumPy array and normalize
        # Ensure dtype is float32 for TensorFlow
        image_array = np.array(image, dtype=np.float32) / 255.0

        # Check array shape after resize
        if image_array.shape != (target_size[0], target_size[1], 3):
             logging.warning(f"Record ID {record_id_for_logging}: Image array shape mismatch after resize for URL {url}. Expected {(target_size[0], target_size[1], 3)}, got {image_array.shape}")
             # Depending on model tolerance, you might return None here
             # return None

        # Prepare the image for the model (add batch dimension)
        input_image = image_array[np.newaxis, ...]

        # Get the feature vector for the image using the global MODEL
        try:
            embedding = MODEL.predict(input_image)
        except Exception as model_err:
            logging.error(f"Record ID {record_id_for_logging}: TensorFlow model prediction error for URL {url}: {model_err}")
            return None

        # Return the flattened embedding vector
        return embedding.flatten()

    except RequestException as req_err:
         # Log network errors during download specifically
         logging.error(f"Record ID {record_id_for_logging}: Network Error downloading image from {url}: {req_err}")
         return None
    except Exception as e:
        # General catch-all for other unexpected errors
        logging.error(f"Record ID {record_id_for_logging}: Unexpected error processing image at {url}: {e}", exc_info=True)
        return None

def upload_to_mongodb(data):
    """
    Uploads the processed data (including s3_url) to MongoDB.
    Converts pandas Timestamps and potentially date strings to Python datetimes.
    """
    MONGO_URI = os.getenv("MONGO_URI")
    # Use DB_NAME consistently if that's the env var name
    DB_NAME = os.getenv("DB_NAME") # Changed from DATABASE_NAME to match your .env likely
    COLLECTION_NAME = os.getenv("COLLECTION_NAME")

    if not all([MONGO_URI, DB_NAME, COLLECTION_NAME]):
        logging.error("MongoDB URI, DB name, or Collection name not set in environment variables.")
        return

    client = None # Initialize client to None for finally block
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000) # Add timeout
        client.admin.command('ismaster') # Verify connection
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logging.info(f"Connected to MongoDB '{DB_NAME}.{COLLECTION_NAME}'.")

        # Prepare data for MongoDB insertion - Handle Date Conversion
        records_to_insert = []
        conversion_errors = 0
        required_id_field = 'none_@file' # Define the intended primary/unique key

        for record in data:
            # Ensure the primary ID field exists, skip record if not
            if required_id_field not in record or pd.isna(record[required_id_field]):
                 logging.warning(f"Skipping record due to missing or null ID field ('{required_id_field}'): {record.get('lot_id', 'Lot ID missing')}")
                 conversion_errors += 1
                 continue

            # Convert 'iso_date' field carefully
            if 'iso_date' in record:
                date_val = record['iso_date']
                if isinstance(date_val, pd.Timestamp):
                    # Convert pandas Timestamp to python datetime
                    record['iso_date'] = date_val.to_pydatetime()
                elif isinstance(date_val, str):
                    # Attempt to parse string date into datetime
                    try:
                        record['iso_date'] = pd.to_datetime(date_val).to_pydatetime()
                    except (ValueError, TypeError) as parse_err:
                        logging.warning(f"Record ID {record[required_id_field]}: Could not parse date string '{date_val}': {parse_err}. Setting to None.")
                        record['iso_date'] = None # Set to None or handle as needed
                        conversion_errors += 1
                elif date_val is None or pd.isna(date_val):
                     record['iso_date'] = None # Explicitly handle None/NaN
                # Add checks for other types if necessary, otherwise assume it might be datetime already or None

            # Convert numeric types that might be numpy types
            for key in ['lo_est', 'hi_est', 'winning_bid']:
                 if key in record and isinstance(record[key], (np.int64, np.float64)):
                     record[key] = record[key].item() # Convert numpy number to Python native type

            # Add the processed record to the list for insertion
            records_to_insert.append(record)

        if conversion_errors > 0:
             logging.warning(f"Encountered {conversion_errors} errors during data type conversion/validation before insert.")

        if not records_to_insert:
             logging.warning("No valid records remaining to insert into MongoDB after preprocessing.")
             return

        # Insert data into MongoDB in batches
        BATCH_SIZE = 500 # Adjust batch size based on record size and network
        total_inserted = 0
        logging.info(f"Attempting to insert {len(records_to_insert)} processed records into MongoDB in batches of {BATCH_SIZE}...")

        try:
            for i in range(0, len(records_to_insert), BATCH_SIZE):
                batch = records_to_insert[i:i + BATCH_SIZE]
                if batch: # Ensure batch is not empty
                    result = collection.insert_many(batch, ordered=False) # Use ordered=False potentially
                    inserted_count = len(result.inserted_ids)
                    total_inserted += inserted_count
                    logging.info(f" > Inserted batch {i//BATCH_SIZE + 1}, {inserted_count} records.")

            logging.info(f"Successfully inserted {total_inserted} records into MongoDB.")

        except BulkWriteError as bwe:
             # Handle potential duplicate key errors if 'none_@file' should be unique
             # bwe.details['writeErrors'] contains info about failed inserts
             logging.error(f"MongoDB BulkWriteError during insertion: {bwe.details}", exc_info=True)
             # You might still count successful writes before the error if ordered=False
             total_inserted = bwe.details.get('nInserted', 0) # Get count of successful inserts from the error
             logging.warning(f"Partial insertion possible: {total_inserted} records might have been inserted before BulkWriteError.")

        except Exception as insert_err:
             logging.error(f"Error inserting data into MongoDB: {insert_err}", exc_info=True)
             # If an error occurs mid-batching, total_inserted reflects count before the error


        # Remove the post-insert update pipeline for string dates, handle it during pre-processing instead.
        # The previous logic converted strings *before* insert.
        # pipeline = [ ... ]
        # filter_query = {"iso_date": {"$type": "string"}}
        # update_result = collection.update_many(filter_query, pipeline)
        # logging.info(f"Attempted post-insert date string conversion: {update_result.modified_count} documents potentially updated.")


    except Exception as e:
        # Catch connection errors or other issues
        logging.error(f"Error connecting to or interacting with MongoDB: {e}", exc_info=True)

    finally:
        if client:
            client.close()
            logging.info("MongoDB connection closed.")


def main():
    try:
        logging.info("Starting main execution flow")

        # Define S3 bucket and file key - consider making these env vars too
        s3_bucket = 'cron-saffron'
        s3_key = 'new_saffron_data.csv'

        # Read CSV from S3
        logging.info(f"Attempting to read CSV from s3://{s3_bucket}/{s3_key}")
        df = read_csv_from_s3(s3_bucket, s3_key) # Removed local_path saving
        if df is None:
            logging.error("Failed to read CSV from S3. Aborting execution.")
            # Consider exiting with an error code: sys.exit(1)
            return # Stop execution
        logging.info(f"Successfully read CSV data. Rows: {len(df)}, Columns: {len(df.columns)}")

        # Validate data
        logging.info("Starting data validation")
        if not validate_data(df):
            logging.error("Data validation failed. Aborting processing.")
            return
        logging.info("Data validation passed successfully")

        # Prepare data for processing
        logging.info("Converting DataFrame to dictionary records")
        # Convert NaN to None for JSON/BSON compatibility
        df = df.replace({np.nan: None})
        data = df.to_dict(orient='records')
        if not data:
            logging.warning("No data records found after conversion (or all filtered out). Aborting processing.")
            return
        logging.info(f"Converted {len(data)} records for processing")

        # Process images and embeddings (uploads to S3 and Pinecone)
        logging.info("Starting image processing, S3 upload, and Pinecone embedding generation")
        process_images_and_embeddings(data)
        logging.info("Completed image processing and embeddings generation step.")

        # Upload to MongoDB (using the 'data' list which now includes 's3_url' for successful records)
        logging.info("Starting MongoDB upload")
        upload_to_mongodb(data)
        logging.info("MongoDB upload step completed.")

        # Delete the file from S3 only if all previous steps were successful
        logging.info(f"Attempting to delete s3://{s3_bucket}/{s3_key}")
        try:
            # Use default client which picks up IAM role
            s3 = boto3.client('s3')
            s3.delete_object(Bucket=s3_bucket, Key=s3_key)
            logging.info(f"Successfully deleted the file s3://{s3_bucket}/{s3_key}")
        except ClientError as e:
             logging.error(f"Failed to delete file s3://{s3_bucket}/{s3_key}: {e}")
        except Exception as e:
             logging.error(f"Unexpected error during S3 file deletion: {e}")


        logging.info("Main execution completed successfully.")

    except Exception as e:
        logging.critical(f"Unexpected critical error in main execution flow: {str(e)}", exc_info=True)
        # Optional: Exit with error code
        # sys.exit(1)
        # raise # Re-raise if running in a context that handles it


if __name__ == "__main__":
    main()
