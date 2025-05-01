# Filename: mongo_upload.py
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
import tf_keras # Assuming this specific import is required
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv
from io import BytesIO
from urllib.parse import urlparse
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
MODEL_URL = 'https://tfhub.dev/google/tf2-preview/mobilenet_v2/classification/4'
MODEL = None # Initialize MODEL to None
try:
    MODEL = tf_keras.Sequential([hub.KerasLayer(MODEL_URL)])
    logging.info(f"TensorFlow Hub model loaded from {MODEL_URL}")
except Exception as e:
    logging.critical(f"Failed to load TensorFlow Hub model: {e}", exc_info=True)
    sys.exit("Model loading failed. Cannot proceed.")


def read_csv_from_s3(bucket_name, object_name, **kwargs):
    """
    Read a CSV file from an S3 bucket using IAM role credentials.

    :param bucket_name: Name of the S3 bucket
    :param object_name: S3 object key/path to the CSV file
    :param kwargs: Additional arguments for pandas.read_csv()
    :return: pandas.DataFrame or None if unsuccessful
    """
    s3_client = boto3.client('s3')
    try:
        logging.info(f"Attempting to get object s3://{bucket_name}/{object_name}")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        logging.info(f"Successfully retrieved object metadata.")

        logging.info("Detecting CSV encoding...")
        try:
            raw_body = response['Body'].read()
            detected_encoding = chardet.detect(raw_body)['encoding']
            if not detected_encoding:
                 detected_encoding = 'utf-8' # Default fallback
            logging.info(f"Detected encoding: {detected_encoding}. Reading CSV...")
            df = pd.read_csv(BytesIO(raw_body), encoding=detected_encoding, **kwargs)
        except UnicodeDecodeError:
            logging.warning(f"Failed with detected encoding {detected_encoding}. Falling back to utf-8.")
            df = pd.read_csv(BytesIO(raw_body), encoding='utf-8', **kwargs) # Try utf-8
        except Exception as read_err:
            logging.error(f"Error reading CSV content: {read_err}", exc_info=True)
            raise

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
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Validation Error: Missing required columns in input data: {', '.join(missing_columns)}")
        return False

    if df['none_@file'].isnull().any():
        logging.error("Validation Error: Found rows with missing 'none_@file'. This is required as an ID.")
        return False

    logging.info("Data validation passed.")
    return True


def process_images_and_embeddings(data):
    """
    Processes images by generating embeddings, uploading images to S3, and embeddings to Pinecone.
    Relies on IAM role for S3 credentials.
    """
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
    PINECONE_INDEX = os.getenv('PINECONE_INDEX')

    if not BUCKET_NAME:
        logging.error("AWS S3 bucket name (BUCKET_NAME) not set in environment variables.")
        return
    if not all([PINECONE_API_KEY, PINECONE_INDEX]):
        logging.error("Pinecone credentials (PINECONE_API_KEY, PINECONE_INDEX) not set in environment variables.")
        return

    # Initialize AWS S3 client using IAM role
    try:
        s3 = boto3.client('s3')
        s3.head_bucket(Bucket=BUCKET_NAME) # Verify access
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
        if (idx + 1) % 100 == 0 or idx == 0 or idx == total_records - 1:
             progress = (idx + 1) / total_records
             logging.info(f"Processing record {idx + 1}/{total_records} ({progress:.1%})")

        image_url = record.get('image_url')
        record_id = record.get('none_@file')

        if not image_url or not record_id:
            logging.warning(f"Record {idx+1} missing 'image_url' or 'none_@file'. Skipping.")
            skipped_count += 1
            continue

        pinecone_id = str(record_id).replace("\\", "/") # Cleaned ID for Pinecone/S3
        s3_key = pinecone_id

        try:
            embeddings = extract_from_image_url(image_url, record_id)
            if embeddings is None:
                skipped_count += 1
                continue
            # Adjust embedding dimension check if needed (e.g., 1001 for this specific model)
            if embeddings.ndim != 1 or embeddings.shape[0] != 1001:
                 logging.warning(f"Record {idx+1} (ID: {record_id}): Unexpected embedding shape {embeddings.shape}. Skipping.")
                 skipped_count += 1
                 continue

            try:
                index.upsert([(pinecone_id, embeddings.tolist())])
            except Exception as pinecone_err:
                logging.error(f"Record {idx+1} (ID: {record_id}): Failed to upsert embeddings to Pinecone: {pinecone_err}")
                pinecone_upload_errors += 1
                # Optionally continue to S3 upload despite Pinecone error

            try:
                response = requests.get(image_url, stream=True, timeout=20)
                response.raise_for_status()
                image_data = response.content
                if not image_data:
                     logging.warning(f"Record {idx+1} (ID: {record_id}): Downloaded empty image data from {image_url}. Skipping S3 upload.")
                     skipped_count += 1
                     continue
            except RequestException as download_err:
                logging.error(f"Record {idx+1} (ID: {record_id}): Error downloading image from URL {image_url}: {download_err}")
                skipped_count += 1
                continue

            try:
                s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=image_data)
                # Add 's3_url' only on successful upload
                s3_url_prefix = f'https://{BUCKET_NAME}.s3.amazonaws.com/'
                record['s3_url'] = s3_url_prefix + s3_key
                processed_count += 1
            except ClientError as s3_err:
                 logging.error(f"Record {idx+1} (ID: {record_id}): Failed to upload image to S3 (Key: {s3_key}): {s3_err}")
                 s3_upload_errors += 1
                 if 's3_url' in record: del record['s3_url'] # Ensure s3_url isn't added on failure

        except Exception as e:
            logging.error(f"Record {idx+1} (ID: {record_id}): Unexpected error during processing loop: {e}", exc_info=True)
            skipped_count += 1
            continue

    logging.info(f"Image processing completed. Processed: {processed_count}, Skipped: {skipped_count}, S3 Errors: {s3_upload_errors}, Pinecone Errors: {pinecone_upload_errors}")


def extract_from_image_url(url, record_id_for_logging="N/A"):
    """
    Extracts image embeddings using TensorFlow Hub model.
    """
    if MODEL is None: # Check if model failed to load initially
        logging.error(f"Record ID {record_id_for_logging}: Cannot extract embeddings, TF model not loaded.")
        return None

    try:
        response = requests.get(url, stream=True, timeout=20)
        response.raise_for_status()
        image_data = response.content
        if not image_data:
             logging.warning(f"Record ID {record_id_for_logging}: Downloaded empty image data from {url}")
             return None

        try:
             image = Image.open(BytesIO(image_data)).convert('RGB')
        except UnidentifiedImageError as image_err:
             logging.error(f"Record ID {record_id_for_logging}: PIL UnidentifiedImageError for URL {url}: {image_err}")
             return None
        except Exception as pil_err:
             logging.error(f"Record ID {record_id_for_logging}: PIL Error opening image from URL {url}: {pil_err}")
             return None

        target_size = (224, 224) # Expected input size for MobileNet V2
        image = image.resize(target_size)
        image_array = np.array(image, dtype=np.float32) / 255.0

        if image_array.shape != (target_size[0], target_size[1], 3):
             logging.warning(f"Record ID {record_id_for_logging}: Image array shape mismatch after resize for URL {url}. Expected {(target_size[0], target_size[1], 3)}, got {image_array.shape}")
             # Handle mismatch if necessary, potentially return None

        input_image = image_array[np.newaxis, ...]

        try:
            embedding = MODEL.predict(input_image)
        except Exception as model_err:
            logging.error(f"Record ID {record_id_for_logging}: TensorFlow model prediction error for URL {url}: {model_err}")
            return None

        return embedding.flatten()

    except RequestException as req_err:
         logging.error(f"Record ID {record_id_for_logging}: Network Error downloading image from {url}: {req_err}")
         return None
    except Exception as e:
        logging.error(f"Record ID {record_id_for_logging}: Unexpected error processing image at {url}: {e}", exc_info=True)
        return None


def upload_to_mongodb(data):
    """
    Uploads the processed data (including s3_url) to MongoDB.
    Converts pandas Timestamps and date strings to Python datetimes.
    """
    MONGO_URI = os.getenv("MONGO_URI")
    DB_NAME = os.getenv("DB_NAME") # Check if this matches your .env (vs DATABASE_NAME)
    COLLECTION_NAME = os.getenv("COLLECTION_NAME")

    if not all([MONGO_URI, DB_NAME, COLLECTION_NAME]):
        logging.error("MongoDB URI, DB name, or Collection name not set in environment variables.")
        return

    client = None
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        client.admin.command('ismaster') # Verify connection
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logging.info(f"Connected to MongoDB '{DB_NAME}.{COLLECTION_NAME}'.")

        records_to_insert = []
        conversion_errors = 0
        required_id_field = 'none_@file'

        for record in data:
            if required_id_field not in record or pd.isna(record[required_id_field]):
                 logging.warning(f"Skipping record due to missing or null ID field ('{required_id_field}'): {record.get('lot_id', 'Lot ID missing')}")
                 conversion_errors += 1
                 continue

            # Convert 'iso_date' field
            if 'iso_date' in record:
                date_val = record['iso_date']
                if isinstance(date_val, pd.Timestamp):
                    record['iso_date'] = date_val.to_pydatetime()
                elif isinstance(date_val, str):
                    try:
                        record['iso_date'] = pd.to_datetime(date_val).to_pydatetime()
                    except (ValueError, TypeError):
                        record['iso_date'] = None # Set invalid date strings to None
                        conversion_errors += 1
                elif date_val is None or pd.isna(date_val):
                     record['iso_date'] = None
                # Assumes other types are already datetime or None

            # Convert numeric types from numpy if necessary
            for key in ['lo_est', 'hi_est', 'winning_bid']:
                 if key in record and isinstance(record[key], (np.int64, np.float64)):
                     record[key] = record[key].item()

            records_to_insert.append(record)

        if conversion_errors > 0:
             logging.warning(f"Encountered {conversion_errors} issues during data type conversion/validation before insert.")

        if not records_to_insert:
             logging.warning("No valid records remaining to insert into MongoDB after preprocessing.")
             return

        BATCH_SIZE = 500
        total_inserted = 0
        logging.info(f"Attempting to insert {len(records_to_insert)} processed records into MongoDB in batches of {BATCH_SIZE}...")

        try:
            for i in range(0, len(records_to_insert), BATCH_SIZE):
                batch = records_to_insert[i:i + BATCH_SIZE]
                if batch:
                    result = collection.insert_many(batch, ordered=False)
                    inserted_count = len(result.inserted_ids)
                    total_inserted += inserted_count
                    logging.info(f" > Inserted batch {i//BATCH_SIZE + 1}, {inserted_count} records.")

            logging.info(f"Successfully inserted {total_inserted} records into MongoDB.")

        except BulkWriteError as bwe:
             logging.error(f"MongoDB BulkWriteError during insertion: {bwe.details}", exc_info=False) # exc_info=False for brevity
             total_inserted = bwe.details.get('nInserted', 0)
             logging.warning(f"Partial insertion possible: {total_inserted} records might have been inserted before BulkWriteError.")
        except Exception as insert_err:
             logging.error(f"Error inserting data into MongoDB: {insert_err}", exc_info=True)

    except Exception as e:
        logging.error(f"Error connecting to or interacting with MongoDB: {e}", exc_info=True)

    finally:
        if client:
            client.close()
            logging.info("MongoDB connection closed.")


def main():
    """
    Main execution function: Read from S3, process, upload, and cleanup.
    """
    start_time = pd.Timestamp.now()
    logging.info(f"=== Starting Main Execution Flow at {start_time} ===")

    # Define S3 source
    s3_bucket = 'cron-saffron'
    s3_key = 'new_saffron_data.csv'

    try:
        logging.info(f"Attempting to read CSV from s3://{s3_bucket}/{s3_key}")
        df = read_csv_from_s3(s3_bucket, s3_key)
        if df is None:
            logging.error("Failed to read CSV from S3. Aborting execution.")
            sys.exit(1)
        logging.info(f"Successfully read CSV data. Rows: {len(df)}, Columns: {len(df.columns)}")

        logging.info("Starting data validation")
        if not validate_data(df):
            logging.error("Data validation failed. Aborting processing.")
            sys.exit(1)
        logging.info("Data validation passed successfully")

        logging.info("Preparing data dictionary records")
        df = df.replace({np.nan: None}) # Ensure NaNs are None for BSON
        data = df.to_dict(orient='records')
        if not data:
            logging.warning("No data records found after conversion. Aborting processing.")
            return # Exit gracefully if no data
        logging.info(f"Converted {len(data)} records for processing")

        logging.info("Starting image processing, S3 upload, and Pinecone embedding generation")
        process_images_and_embeddings(data) # Modifies 'data' list with 's3_url'
        logging.info("Completed image processing and embeddings generation step.")

        logging.info("Starting MongoDB upload")
        upload_to_mongodb(data)
        logging.info("MongoDB upload step completed.")

        logging.info(f"Attempting to delete s3://{s3_bucket}/{s3_key}")
        try:
            s3 = boto3.client('s3')
            s3.delete_object(Bucket=s3_bucket, Key=s3_key)
            logging.info(f"Successfully deleted the file s3://{s3_bucket}/{s3_key}")
        except ClientError as e:
             logging.error(f"Failed to delete file s3://{s3_bucket}/{s3_key}: {e}")
        except Exception as e:
             logging.error(f"Unexpected error during S3 file deletion: {e}")

        end_time = pd.Timestamp.now()
        logging.info(f"=== Main execution completed successfully at {end_time} (Duration: {end_time - start_time}) ===")

    except Exception as e:
        logging.critical(f"=== Unhandled critical error in main execution flow: {str(e)} ===", exc_info=True)
        sys.exit(1) # Exit with error status


if __name__ == "__main__":
    # Ensure model is loaded before calling main if main relies on it implicitly
    if MODEL is None:
         logging.critical("Exiting because TensorFlow model failed to load.")
         sys.exit(1)
    main()
