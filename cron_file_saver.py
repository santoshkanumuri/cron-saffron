from pymongo import MongoClient
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import logging
import boto3

load_dotenv()

def download_similarities_data():
    logging.info("Starting data download...")
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("DB_NAME")]
    collection = db[os.getenv("COLLECTION_NAME")]
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    df.to_csv("./files/similarities.csv", index=False)
    logging.info("Data downloaded successfully in the code directory")


def download_bid_data():
    logging.info("Starting bid data download...")
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("DB_NAME")]
    collection = db[os.getenv("SAFFRON_BID_COLLECTION_NAME")]
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    df.to_csv("./files/bid_data.csv", index=False)
    logging.info("Data downloaded successfully in the code directory")


def download_transformed_data():
    logging.info("Starting transformed data download...")
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("DB_NAME")]
    collection = db[os.getenv("SAFFRON_BID_COLLECTION_NAME")]
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))

    def add_year_to_bid_datetime(df):
        # Convert 'iso_date' to string and split to keep date part (YYYY-MM-DD)
        df['iso_date'] = df['iso_date'].astype(str).str.split(' ').str[0]

        # Convert 'iso_date' to datetime
        df['iso_date'] = pd.to_datetime(df['iso_date'], format='%Y-%m-%d', errors='coerce')

        # Parse 'bid_datetime' into datetime (without year)
        df['bid_datetime'] = pd.to_datetime(
            df['bid_datetime'],
            format='%b %d %I:%M:%S %p',
            errors='coerce'
        )

        # Replace the year in 'bid_datetime' with the year from 'iso_date'
        df['bid_datetime'] = df.apply(
            lambda row: row['bid_datetime'].replace(year=row['iso_date'].year)
            if pd.notna(row['bid_datetime']) and pd.notna(row['iso_date'])
            else row['bid_datetime'],
            axis=1
        )

        return df

    def transform_bid_data(df, output_csv):
        constant_columns = [
            'auction_house', 'lot_link', 'lot_id', 'auction_date', 'iso_date',
            's3_url', 'winning_bid', 'lo_est', 'hi_est', 'artist_name'
        ]

        transformed_rows = []
        total_rows = len(df)
        if total_rows == 0:
            progress_bar.progress(100)
            return

        # We'll update progress within the loop
        logging.info("Transforming data...")
        for i, (_, row) in enumerate(df.iterrows(), 1):
            lot_data = {col: row[col] for col in constant_columns}
            bid_index = 1

            while True:
                bid_usd = row.get(f'bid_{bid_index}_usd', None)
                bid_rs = row.get(f'bid_{bid_index}_rs', None)
                bid_name = row.get(f'bid_{bid_index}_name', None)
                bid_type = row.get(f'bid_{bid_index}_type', None)
                bid_datetime = row.get(f'bid_{bid_index}_datetime', None)

                # If all bid data is empty, break the loop
                if pd.isna(bid_usd) and pd.isna(bid_rs) and pd.isna(bid_name) and pd.isna(bid_type) and pd.isna(bid_datetime):
                    break

                bid_data = lot_data.copy()  # Copy the constant lot data
                bid_data['bid_number'] = bid_index
                bid_data['bid_usd'] = bid_usd
                bid_data['bid_rs'] = bid_rs
                bid_data['bid_name'] = bid_name
                bid_data['bid_type'] = bid_type
                bid_data['bid_datetime'] = bid_datetime

                transformed_rows.append(bid_data)
                bid_index += 1

        transformed_df = pd.DataFrame(transformed_rows)
        transformed_df = add_year_to_bid_datetime(transformed_df)
        transformed_df.to_csv(output_csv, index=False)
    transform_bid_data(df, './files/transformed_bid_data.csv')
    logging.info("Data downloaded and transformed successfully in the code directory")

def upload_to_s3(file_path, bucket_name, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_path)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    try:
        response = s3_client.upload_file(file_path, bucket_name, object_name)
    except Exception as e:
        logging.error(f"Failed to upload {file_path} to S3: {str(e)}")
        return False
    logging.info(f"{file_path} uploaded successfully to S3 bucket: {bucket_name}")
    #return s3 object link
    return True


def download_data():
    download_similarities_data()
    download_bid_data()
    download_transformed_data()
    link1 = upload_to_s3('./files/similarities.csv', 'scraped-art-data')
    link2 = upload_to_s3('./files/bid_data.csv', 'scraped-art-data')
    link3 = upload_to_s3('./files/transformed_bid_data.csv', 'scraped-art-data')
    if link1 and link2 and link3:
        logging.info("All files uploaded successfully to S3")
    else:
        logging.error("Failed to upload all files to S3")




if __name__ == "__main__":
    download_data()
