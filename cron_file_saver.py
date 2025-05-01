import os
import logging
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Load environment variables
load_dotenv()

# Configure basic logging if not already configured elsewhere
# If you have a central logging setup, this might not be needed here
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

# --- Directory Definition ---
# Define the output directory relative to the script location
OUTPUT_DIR = "./files"

def ensure_dir_exists(directory_path):
    """Creates the directory if it doesn't exist."""
    try:
        os.makedirs(directory_path, exist_ok=True)
        logging.info(f"Ensured directory exists: {directory_path}")
    except OSError as e:
        logging.error(f"Error creating directory {directory_path}: {e}")
        raise # Re-raise the error if directory creation fails critically

def download_similarities_data():
    """Downloads similarities data from MongoDB and saves it to a CSV file."""
    logging.info("Starting similarities data download...")
    output_file = os.path.join(OUTPUT_DIR, "similarities.csv")
    client = None
    try:
        client = MongoClient(os.getenv("MONGO_URI"), serverSelectionTimeoutMS=10000)
        client.admin.command('ismaster') # Verify connection
        db = client[os.getenv("DB_NAME")]
        collection = db[os.getenv("COLLECTION_NAME")] # Assumes COLLECTION_NAME holds similarities
        cursor = collection.find({})
        df = pd.DataFrame(list(cursor))
        if not df.empty:
            ensure_dir_exists(OUTPUT_DIR) # Ensure directory exists before writing
            df.to_csv(output_file, index=False)
            logging.info(f"Similarities data downloaded successfully to {output_file}")
        else:
            logging.warning("No similarities data found in MongoDB collection.")
    except Exception as e:
        logging.error(f"Failed to download similarities data: {e}", exc_info=True)
    finally:
        if client:
            client.close()

def download_bid_data():
    """Downloads raw bid data from MongoDB and saves it to a CSV file."""
    logging.info("Starting raw bid data download...")
    output_file = os.path.join(OUTPUT_DIR, "bid_data.csv")
    client = None
    try:
        client = MongoClient(os.getenv("MONGO_URI"), serverSelectionTimeoutMS=10000)
        client.admin.command('ismaster')
        db = client[os.getenv("DB_NAME")]
        collection = db[os.getenv("SAFFRON_BID_COLLECTION_NAME")] # Assumes this collection holds bid data
        cursor = collection.find({})
        df = pd.DataFrame(list(cursor))
        if not df.empty:
            ensure_dir_exists(OUTPUT_DIR) # Ensure directory exists before writing
            df.to_csv(output_file, index=False)
            logging.info(f"Raw bid data downloaded successfully to {output_file}")
        else:
            logging.warning("No bid data found in MongoDB collection.")
    except Exception as e:
        logging.error(f"Failed to download raw bid data: {e}", exc_info=True)
    finally:
        if client:
            client.close()


# --- Transformation Helper Functions (extracted for clarity) ---

def add_year_to_bid_datetime(df):
    """Adds the year from 'iso_date' to the 'bid_datetime' column."""
    if 'iso_date' not in df.columns or 'bid_datetime' not in df.columns:
         logging.warning("Missing 'iso_date' or 'bid_datetime' columns for year addition.")
         return df

    # Attempt conversion, coercing errors
    df['iso_date_dt'] = pd.to_datetime(df['iso_date'], errors='coerce')
    df['bid_datetime_dt'] = pd.to_datetime(df['bid_datetime'], format='%b %d %I:%M:%S %p', errors='coerce')

    # Apply year replacement where both dates are valid
    valid_dates_mask = df['iso_date_dt'].notna() & df['bid_datetime_dt'].notna()
    df.loc[valid_dates_mask, 'bid_datetime_final'] = df[valid_dates_mask].apply(
        lambda row: row['bid_datetime_dt'].replace(year=row['iso_date_dt'].year),
        axis=1
    )
    # Keep original bid_datetime where conversion wasn't possible or needed
    df['bid_datetime_final'].fillna(df['bid_datetime_dt'], inplace=True)

    # Clean up temporary columns and potentially update original column
    df['bid_datetime'] = df['bid_datetime_final'] # Overwrite original or keep separate
    df.drop(columns=['iso_date_dt', 'bid_datetime_dt', 'bid_datetime_final'], inplace=True)

    return df

def transform_bid_data_structure(df):
    """Transforms wide bid data into a long format."""
    constant_columns = [
        'auction_house', 'lot_link', 'lot_id', 'auction_date', 'iso_date',
        's3_url', 'winning_bid', 'lo_est', 'hi_est', 'artist_name'
    ]
    # Ensure constant columns exist, handle missing ones gracefully if needed
    valid_constant_columns = [col for col in constant_columns if col in df.columns]
    missing_constant_cols = set(constant_columns) - set(valid_constant_columns)
    if missing_constant_cols:
        logging.warning(f"Missing constant columns for transformation: {missing_constant_cols}")

    transformed_rows = []
    logging.info("Transforming bid data structure...")
    for _, row in df.iterrows():
        # Get constant data for the lot, handling potentially missing columns
        lot_data = {col: row.get(col) for col in valid_constant_columns}
        bid_index = 1

        while True:
            # Check for existence of any bid_{index}_* column for this index
            bid_cols_for_index = [f'bid_{bid_index}_{suffix}' for suffix in ['usd', 'rs', 'name', 'type', 'datetime']]
            if not any(col in row and pd.notna(row[col]) for col in bid_cols_for_index):
                 # If no non-NA data exists for this bid index, assume end of bids for this lot
                 break

            bid_data = lot_data.copy() # Start with constant lot data
            bid_data['bid_number'] = bid_index
            bid_data['bid_usd'] = row.get(f'bid_{bid_index}_usd')
            bid_data['bid_rs'] = row.get(f'bid_{bid_index}_rs')
            bid_data['bid_name'] = row.get(f'bid_{bid_index}_name')
            bid_data['bid_type'] = row.get(f'bid_{bid_index}_type')
            bid_data['bid_datetime'] = row.get(f'bid_{bid_index}_datetime')

            transformed_rows.append(bid_data)
            bid_index += 1

    if not transformed_rows:
        logging.warning("No bid data could be transformed (input might be empty or lack bid columns).")
        return pd.DataFrame() # Return empty DataFrame

    transformed_df = pd.DataFrame(transformed_rows)
    logging.info(f"Transformation complete. Created {len(transformed_df)} rows.")
    return transformed_df

# --- Main Download Function for Transformed Data ---

def download_transformed_data():
    """Downloads bid data, transforms it, and saves to CSV."""
    logging.info("Starting transformed bid data download and processing...")
    output_file = os.path.join(OUTPUT_DIR, "transformed_bid_data.csv")
    client = None
    try:
        client = MongoClient(os.getenv("MONGO_URI"), serverSelectionTimeoutMS=10000)
        client.admin.command('ismaster')
        db = client[os.getenv("DB_NAME")]
        collection = db[os.getenv("SAFFRON_BID_COLLECTION_NAME")]
        cursor = collection.find({})
        df = pd.DataFrame(list(cursor))

        if not df.empty:
            transformed_df = transform_bid_data_structure(df)
            if not transformed_df.empty:
                final_df = add_year_to_bid_datetime(transformed_df)
                ensure_dir_exists(OUTPUT_DIR) # Ensure directory exists before writing
                final_df.to_csv(output_file, index=False)
                logging.info(f"Transformed bid data saved successfully to {output_file}")
            else:
                logging.warning("Transformation resulted in an empty DataFrame. No file saved.")
        else:
            logging.warning("No bid data found in MongoDB collection to transform.")

    except Exception as e:
        logging.error(f"Failed to download and transform bid data: {e}", exc_info=True)
    finally:
        if client:
            client.close()

# --- S3 Upload Function ---

def upload_to_s3(file_path, bucket_name, object_name=None):
    """
    Uploads a file to S3 using IAM role credentials.

    :param file_path: Path to the file to upload.
    :param bucket_name: Name of the S3 bucket.
    :param object_name: S3 object name. If None, uses the base filename.
    :return: True if upload successful, False otherwise.
    """
    if not os.path.exists(file_path):
        logging.error(f"Cannot upload file. File not found: {file_path}")
        return False

    if object_name is None:
        object_name = os.path.basename(file_path)

    # Use default session which picks up IAM role
    s3_client = boto3.client('s3')

    try:
        # Verify bucket access before upload attempt (optional but good practice)
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"Uploading {file_path} to s3://{bucket_name}/{object_name}...")
        s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info(f"Successfully uploaded {file_path} to S3.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
             logging.error(f"S3 Upload Error: Bucket '{bucket_name}' does not exist.")
        elif e.response['Error']['Code'] == 'AccessDenied':
             logging.error(f"S3 Upload Error: Access Denied for bucket '{bucket_name}'. Check IAM role permissions.")
        else:
             logging.error(f"S3 Upload Error: {e}", exc_info=True)
        return False
    except NoCredentialsError:
         logging.error("S3 Upload Error: AWS credentials/IAM role not found.")
         return False
    except Exception as e:
        logging.error(f"Failed to upload {file_path} to S3: {str(e)}", exc_info=True)
        return False

# --- Main Orchestration Function ---

def download_and_upload_all_data():
    """Downloads all required data, transforms bid data, and uploads results to S3."""
    logging.info("=== Starting Data Download and S3 Upload Process ===")

    # Download data first
    download_similarities_data()
    download_bid_data()
    download_transformed_data() # Downloads raw data and saves transformed version

    # Define files to upload
    files_to_upload = {
        "similarities": os.path.join(OUTPUT_DIR, "similarities.csv"),
        "bid_data": os.path.join(OUTPUT_DIR, "bid_data.csv"),
        "transformed_bid_data": os.path.join(OUTPUT_DIR, "transformed_bid_data.csv")
    }

    # Upload files to S3
    s3_bucket = 'scraped-art-data' # Define bucket name clearly
    all_successful = True

    for key, file_path in files_to_upload.items():
        logging.info(f"--- Uploading {key} data ---")
        if not upload_to_s3(file_path, s3_bucket):
            logging.error(f"Upload failed for {key} ({file_path})")
            all_successful = False
        # Consider adding a small delay between uploads if needed

    if all_successful:
        logging.info("=== All files uploaded successfully to S3 ===")
    else:
        logging.error("=== One or more file uploads to S3 failed ===")

    return all_successful


if __name__ == "__main__":
    # Call the main orchestration function
    success = download_and_upload_all_data()
    # Optionally exit with status code based on success
    # sys.exit(0 if success else 1)
