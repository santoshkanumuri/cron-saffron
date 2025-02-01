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
import tf_keras
from pymongo import MongoClient
from dotenv import load_dotenv
from io import BytesIO
from urllib.parse import urlparse
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
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
MODEL = tf_keras.Sequential([hub.KerasLayer(MODEL_URL)])

def main():
    try:
        logging.info("Starting main execution flow")

        # Read CSV from S3
        logging.info("Attempting to read CSV from S3 bucket 'cron-saffron'")
        df = read_csv_from_s3('cron-saffron', 'new_saffron_data.csv')
        if df is None:
            logging.error("Failed to read CSV from S3. Aborting execution.")
            return
        logging.info(f"Successfully read CSV data. Rows: {len(df)}, Columns: {len(df.columns)}")

        # Validate data
        logging.info("Starting data validation")
        if not validate_data(df):
            logging.error("Data validation failed. Aborting processing.")
            return
        logging.info("Data validation passed successfully")

        # Prepare data for processing
        logging.info("Converting DataFrame to dictionary records")
        data = df.to_dict(orient='records')
        if not data:
            logging.warning("No data records found after conversion. Aborting processing.")
            return
        logging.info(f"Converted {len(data)} records for processing")

        # Process images and embeddings
        logging.info("Starting image processing and embedding generation")
        process_images_and_embeddings(data)
        logging.info("Completed image processing and embeddings generation")

        # Upload to MongoDB
        logging.info("Starting MongoDB upload")
        upload_to_mongodb(data)
        logging.info("MongoDB upload completed successfully")

        logging.info("Main execution completed successfully")
        # delete the file from s3
        s3 = boto3.client('s3')
        s3.delete_object(Bucket='cron-saffron', Key='new_saffron_data.csv')
        logging.info("Deleted the file from S3 bucket 'cron-saffron'")

    except Exception as e:
        logging.critical(f"Unexpected error in main execution: {str(e)}", exc_info=True)
        raise
def read_csv_from_s3(bucket_name, object_name, local_path=None, **kwargs):
    """
    Read a CSV file from an S3 bucket and optionally save it locally

    :param bucket_name: Name of the S3 bucket
    :param object_name: S3 object key/path to the CSV file
    :param local_path: (Optional) Local path to save the CSV file
    :param kwargs: Additional arguments for pandas.read_csv()
    :return: pandas.DataFrame or None if unsuccessful
    """
    s3_client = boto3.client('s3')

    try:
        # Get the CSV object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)

        # Read CSV directly into DataFrame
        df = pd.read_csv(response['Body'], **kwargs)

        # Optionally save to local file
        if local_path:
            response['Body'].read().decode('utf-8')
            s3_client.download_file(bucket_name, object_name, local_path)
            print(f"File saved to {local_path}")

        print(f"Successfully read CSV from s3://{bucket_name}/{object_name}")
        return df

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            print(f"Error: File {object_name} not found in bucket {bucket_name}")
        elif error_code == 'NoSuchBucket':
            print(f"Error: Bucket {bucket_name} does not exist")
        else:
            print(f"AWS Client Error: {str(e)}")
        return None
    except NoCredentialsError:
        print("Error: AWS credentials not found")
        return None
    except pd.errors.ParserError as e:
        print(f"CSV Parsing Error: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        return None

def read_uploaded_file(uploaded_file):
    """
    Reads the uploaded file into a pandas DataFrame.
    Automatically detects the file type and encoding.
    """
    try:
        # Detect file type using MIME type
        file_type = detect_file_type(uploaded_file)
        if not file_type:
            logging.error(f"Unsupported file type for file: {uploaded_file.name}")
            return None
        logging.write(f"**Detected File Type:** {file_type}")

        if file_type == 'CSV':
            df = read_csv_file(uploaded_file)
        elif file_type == 'Excel':
            df = pd.read_excel(uploaded_file, engine='openpyxl')
        else:
            logging.error("Unsupported file type.")
            return None
        return df

    except Exception as e:
        logging.error(f"Error reading file {uploaded_file.name}: {e}")
        return None

def detect_file_type(uploaded_file):
    """
    Detects the file type based on the file extension.
    """
    extension = os.path.splitext(uploaded_file.name)[1].lower()
    if extension == '.csv':
        return 'CSV'
    elif extension in ['.xls', '.xlsx']:
        return 'Excel'
    else:
        return None

def read_csv_file(uploaded_file):
    """
    Reads a CSV file, attempting to detect the correct encoding.
    """
    # Try to detect encoding
    raw_data = uploaded_file.read(100000)
    uploaded_file.seek(0)
    result = chardet.detect(raw_data)
    encoding = result['encoding']
    confidence = result['confidence']

    if confidence < 0.8:
        logging.warning("Low confidence in encoding detection. Trying common encodings.")
        for enc in ['utf-8', 'ISO-8859-1', 'Windows-1252']:
            try:
                df = pd.read_csv(uploaded_file, encoding=enc)
                uploaded_file.seek(0)
                logging.info(f"Successfully read the file using encoding: {enc}")
                return df
            except UnicodeDecodeError:
                uploaded_file.seek(0)
                continue
        logging.error("Unable to read the file with common encodings.")
        return None
    else:
        logging.info(f"Detected encoding: {encoding} (Confidence: {confidence:.2%})")
        df = pd.read_csv(uploaded_file, encoding=encoding)
        uploaded_file.seek(0)
        return df

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
        logging.error(f"Missing required columns: {', '.join(missing_columns)}")
        return False
    return True

def process_images_and_embeddings(data):
    """
    Processes images by generating embeddings, uploading images to S3, and embeddings to Pinecone.
    """
    # Get AWS credentials from environment variables
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    BUCKET_NAME = os.getenv('BUCKET_NAME')

    # Get Pinecone API key and environment from environment variables
    PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
    PINECONE_INDEX = os.getenv('PINECONE_INDEX')

    # Validate credentials
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME]):
        logging.error("AWS credentials or bucket name not set in environment variables.")
        return

    if not all([PINECONE_API_KEY, PINECONE_INDEX]):
        logging.error("Pinecone credentials not set in environment variables.")
        return

    # Initialize AWS S3 client
    try:
        s3 = boto3.client('s3',
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        logging.info("Connected to AWS S3.")
    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.error(f"AWS credentials error: {e}")
        return

    # Initialize Pinecone
    pc=Pinecone(api_key=PINECONE_API_KEY)
    try:

        index = pc.Index(PINECONE_INDEX)
        logging.info(f"Connected to Pinecone index '{PINECONE_INDEX}'.")
    except Exception as e:
        logging.error(f"Pinecone connection error: {e}")
        return

    total_records = len(data)
    for idx, record in enumerate(data):
        logging.info(f"Processing record {idx + 1} of {total_records}")
        progress = (idx + 1) / total_records
        logging.info(f"Progress: {progress:.2%}")

        image_url = record.get('image_url')
        none_at_file = record.get('none_@file')

        if not image_url or not none_at_file:
            logging.warning(f"Record missing 'image_url' or 'none_@file'. Skipping record: {record}")
            continue

        try:
            # Generate embeddings
            embeddings = extract_from_image_url(image_url)
            if embeddings is None or len(embeddings) != 1001:
                logging.warning(f"Failed to generate embeddings for image_url: {image_url}")
                continue

            # Upload embeddings to Pinecone
            index.upsert([(none_at_file, embeddings.tolist())])
            logging.info(f"Uploaded embeddings to Pinecone for id: {none_at_file}")

            # Download image
            response = requests.get(image_url, stream=True, timeout=10)
            response.raise_for_status()
            image_data = response.content

            # Upload image to S3
            none_at_file_s3 = none_at_file.replace("\\", "/")
            s3.put_object(Bucket=BUCKET_NAME, Key=none_at_file_s3, Body=image_data)
            logging.info(f"Uploaded image to S3 bucket '{BUCKET_NAME}' with key '{none_at_file_s3}'")

            # Add 's3_url' to record
            s3_url_prefix = f'https://{BUCKET_NAME}.s3.amazonaws.com/'
            record['s3_url'] = s3_url_prefix + none_at_file_s3

        except RequestException as e:
            logging.error(f"Error downloading image from URL {image_url}: {e}")
            continue
        except Exception as e:
            logging.error(f"Error processing record with none_@file: {none_at_file}: {e}")
            continue

    logging.info("Image processing completed.")

def extract_from_image_url(url):
    """
    Extracts image embeddings using TensorFlow Hub model.
    """
    try:
        logging.info(f"Processing image: {url}")

        # Download image
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        image_data = response.content

        # Open the image using PIL
        image = Image.open(BytesIO(image_data)).convert('RGB')

        # Resize the image to the desired shape
        image = image.resize((224, 224))

        # Convert the image to a NumPy array
        image_array = np.array(image) / 255.0

        # Prepare the image for the model
        input_image = image_array[np.newaxis, ...]

        # Get the feature vector for the image
        embedding = MODEL.predict(input_image)

        return embedding.flatten()

    except UnidentifiedImageError as e:
        logging.error(f"UnidentifiedImageError for URL {url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error processing image at {url}: {e}")
        return None

def upload_to_mongodb(data):
    """
    Uploads the data to MongoDB.
    """
    MONGO_URI = os.getenv("MONGO_URI")
    DB_NAME = os.getenv("DATABASE_NAME")
    COLLECTION_NAME = os.getenv("COLLECTION_NAME")

    if not all([MONGO_URI, DB_NAME, COLLECTION_NAME]):
        logging.error("MongoDB credentials or database/collection name not set in environment variables.")
        return

    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logging.info(f"Connected to MongoDB database '{DB_NAME}', collection '{COLLECTION_NAME}'.")

        # Insert data into MongoDB
        BATCH_SIZE = 1000
        total_inserted = 0
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            # **Convert 'iso_date' in batch to datetime objects**
            for record in batch:
                if 'iso_date' in record and isinstance(record['iso_date'], pd.Timestamp):
                    record['iso_date'] = record['iso_date'].to_pydatetime()
            result = collection.insert_many(batch)
            total_inserted += len(result.inserted_ids)

        pipeline = [
                        {
                            "$set": {
                            "iso_date": {
                                "$dateFromString": {"dateString": "$iso_date"}
                                        }
                                    }
                        }
                    ]

        # Identify documents where iso_date is of type string and update them
        filter_query = {"iso_date": {"$type": "string"}}
        update_result = collection.update_many(filter_query, pipeline)
        logging.info(f"Updated {update_result.modified_count} documents with iso_date as string.")
        logging.info(f"Successfully inserted {total_inserted} records into MongoDB.")


    except Exception as e:
        logging.error(f"Error inserting data into MongoDB: {e}")

    finally:
        client.close()
        logging.info("MongoDB connection closed.")


if __name__ == "__main__":
    main()
