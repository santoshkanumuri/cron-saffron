import pandas as pd
from pymongo import MongoClient
import requests
import logging
import json
from bs4 import BeautifulSoup
import re
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import os
import dotenv
import time

dotenv.load_dotenv()
# MongoDB Connection
MONGO_URI = os.getenv('MONGO_URI')

client = MongoClient(MONGO_URI)
db = client[os.getenv('DB_NAME')]
art_collection = db[os.getenv('COLLECTION_NAME')]
saffron_bid_data_collection = db[os.getenv('SAFFRON_BID_COLLECTION_NAME')]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s', filename='bid_data.log')

import pandas as pd

def transform_bid_data(original_df):
    """
    Transforms the original DataFrame with bid data into a transposed format.

    Parameters:
    original_df (pd.DataFrame): Original DataFrame containing auction and bid data.

    Returns:
    pd.DataFrame: Transformed DataFrame with bid data transposed into rows.
    """
    # Define columns that are part of the initial data
    initial_data_cols = ['auction_house', 'lot_link', 'lot_id', 'auction_date',
                         'iso_date', 's3_url', 'winning_bid', 'lo_est', 'hi_est']
    initial_data = original_df[initial_data_cols]

    # Extract bid columns (those with 'bid_' prefix)
    bid_data = original_df.drop(columns=initial_data_cols)

    # Reshape bid data by stacking it and then splitting it into respective bid columns
    bid_data_stacked = bid_data.stack().reset_index()
    bid_data_stacked.columns = ['index', 'bid_attribute', 'value']

    # Extract bid index and attribute from 'bid_attribute' column
    bid_data_stacked['bid_index'] = bid_data_stacked['bid_attribute'].str.extract(r'bid_(\d+)_')[0]
    bid_data_stacked['bid_index'] = pd.to_numeric(bid_data_stacked['bid_index'], errors='coerce')
    bid_data_stacked['attribute'] = bid_data_stacked['bid_attribute'].str.extract(r'bid_\d+_(.*)')[0]

    # Drop rows where bid_index or attribute extraction was unsuccessful
    bid_data_stacked.dropna(subset=['bid_index', 'attribute'], inplace=True)

    # Convert 'bid_index' to integer safely after handling NaNs
    bid_data_stacked['bid_index'] = bid_data_stacked['bid_index'].astype(int)

    # Pivot to get each bid with 'usd', 'rs', 'type', 'datetime' as columns
    bid_data_pivoted = bid_data_stacked.pivot(index=['index', 'bid_index'], columns='attribute', values='value').reset_index()

    # Merge with initial data
    transformed_data = bid_data_pivoted.merge(initial_data, left_on='index', right_index=True).drop(columns=['index'])

    # Reorder columns for clarity
    transformed_data = transformed_data[['auction_house', 'lot_link', 'lot_id', 'auction_date', 'iso_date', 's3_url',
                                         'winning_bid', 'lo_est', 'hi_est', 'bid_index', 'usd', 'rs', 'type', 'datetime']]

    # Convert 'lot_id' to integer
    transformed_data['lot_id'] = transformed_data['lot_id'].astype(int)

    return transformed_data


# Helper Functions
def extract_auction_and_lot_number(url, lot_id):
    """
    Extracts lot_number from the given URL.
    Example URL: "https://www.saffronart.com/auctions/PostWork.aspx?l=1404&auctionid=18"
    """
    # Extract 'l' parameter (lot_number)
    if not url:
        return None, int(lot_id)

    #handle Nan
    if pd.isnull(url):
        return None, int(lot_id)

    lot_match = re.search(r'l=(\d+)', url)
    lot_number = lot_match.group(1) if lot_match else None

    return lot_number, lot_id


# construct context key
def construct_context_key(auction_id, lot_id):
    """
    Constructs a dynamic contextKey using auction_id and lot_number.
    """
    context_key = f"{auction_id};{int(lot_id)};;8d617c5b-a0e9-4e77-a771-4bb865e63590"
    logging.info(f"Constructed contextKey: {context_key}")
    return context_key

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def fetch_and_parse_bid_data(lot_link, lot_id):
    """
    Fetches bidding data from the Saffron Art API and parses it into a list of bid dictionaries.

    Parameters:
        lot_link (str): The link to the lot.
        lot_id (str): The ID of the lot.

    Returns:
        list: A list of bid dictionaries containing bid details.
    """
    headers = {
        'Referer': 'https://www.saffronart.com/',
        'Content-Type': 'application/json',
        'Origin': 'https://www.saffronart.com',
        'User-Agent': (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36"
        ),
        'Accept': '*/*',
        'Accept-Language': "en-US,en;q=0.9",
        'X-Requested-With': "XMLHttpRequest",
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
    }

    # Extract auction_id and lot_number
    auction_id, lot_number = extract_auction_and_lot_number(lot_link, lot_id)

    if not auction_id or not lot_number:
        logging.error(f"Could not extract auction_id or lot_number from lot_link: {lot_link}")
        return []

    # Construct the contextKey using auction_id and lot_number
    context_key = construct_context_key(auction_id, lot_number)

    logging.debug(f"Using contextKey: {context_key} for lot_id {int(lot_id)}")

    data = {
        "contextKey": context_key
    }

    url = 'https://www.saffronart.com/webservices/DurationCatalogService.asmx/GetBidHistory'

    try:
        logging.debug("Sending POST request to %s for lot_id %s", url, int(lot_id))
        response = requests.post(url, headers=headers, json=data)
        logging.debug("Received response with status code %d for lot_id %s", response.status_code, int(lot_id))

        # Raise an exception if the HTTP request returned an unsuccessful status code
        response.raise_for_status()

        # Parse the JSON response
        try:
            json_response = response.json()
            logging.debug("Successfully parsed JSON response for lot_id %s", int(lot_id))
        except json.JSONDecodeError as e:
            logging.error("Failed to parse response as JSON for lot_id %s: %s", lot_id, e)
            logging.debug("Response content for lot_id %s: %s", lot_id, response.text)
            return []

        # Extract the HTML content from the 'd' key
        html_content = json_response.get('d', '')
        if not html_content:
            logging.error("No HTML content found in the 'd' key for lot_id %s", lot_id)
            return []

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find the div containing the bid history
        bid_history_div = soup.find('div', {'id': 'bidHistoryDiv'})
        if not bid_history_div:
            logging.error("Bid history div not found in the HTML content for lot_id %s", lot_id)
            return []

        # Extract the rows containing bid data
        bid_rows = bid_history_div.find_all('tr')

        # Initialize a list to store bid dictionaries
        bids = []

        # Iterate over each row, skipping the header row(s)
        for tr in bid_rows:
            td_elements = tr.find_all('td')
            if len(td_elements) >= 6:
                # Extract text from each 'td' element
                bid_number = td_elements[0].get_text(strip=True)
                nickname = td_elements[1].get_text(strip=True)
                amount_usd = td_elements[2].get_text(strip=True)
                amount_rs = td_elements[3].get_text(strip=True)
                bid_type = td_elements[4].get_text(strip=True)
                bid_datetime = td_elements[5].get_text(strip=True)

                # Skip header rows where 'Bid Number' is non-numeric or empty
                if not bid_number.isdigit():
                    continue

                # Clean and convert amounts
                amount_usd = amount_usd.replace('$', '').replace(',', '').strip()
                amount_rs = amount_rs.replace(',', '').strip()

                # Convert amounts to float if possible
                try:
                    amount_usd = float(amount_usd) if amount_usd else None
                except ValueError:
                    amount_usd = None

                try:
                    amount_rs = float(amount_rs) if amount_rs else None
                except ValueError:
                    amount_rs = None

                # Append the bid dictionary
                bids.append({
                    'Bid Number': int(bid_number),
                    'Nickname': nickname,
                    'Amount($)': amount_usd,
                    'Amount(Rs)': amount_rs,
                    'Type': bid_type,
                    'Date & Time(US EST)': bid_datetime
                })

        if not bids:
            logging.warning("No valid bids found for lot_id %s", int(lot_id))

        logging.debug(f"Extracted {len(bids)} bids for lot_id {int(lot_id)}")
        return bids

    except requests.exceptions.HTTPError as http_err:
        logging.error("HTTP error occurred for lot_id %s: %s",lot_id, http_err)
        logging.debug("Response content for lot_id %s: %s", lot_id, response.text)
        return []
    except requests.exceptions.RequestException as e:
        logging.error("An error occurred during the HTTP request for lot_id %s: %s",lot_id, e)
        return []
    except Exception as e:
        logging.error("An unexpected error occurred for lot_id %s: %s",lot_id, e)
        return []


# Streamlit App
def bid_main():

    logging.info("Starting bid data fetching process...")
    logging.info("Fetching bid data. Please wait...")

    # Initialize a list to store all data
    all_data = []

    # Fetch existing lot_links from 'saffron_bid_data' to avoid duplicates
    try:
        existing_lot_links = saffron_bid_data_collection.distinct('lot_link')
        logging.info(f"Fetched {len(existing_lot_links)} existing lot_links from 'saffron_bid_data'")
    except Exception as e:
        logging.error(f"Error fetching existing lot_links from MongoDB: {e}")
        return

    # Query documents where auction_house is "Saffron Art" and lot_link not in existing_lot_links and not NaN
    query = {
        'auction_house': 'Saffron Art',
        'lot_link': {'$nin': existing_lot_links +[float('nan')]},
    }

    # Remove the limit to process all lots
    # limit = 3  # Removed to process all available documents

    try:
        logging.warning("Fetching documents from MongoDB. This may take a while...")
        documents = art_collection.find(query)
        total_documents = documents.count() if hasattr(documents, 'count') else len(list(documents))
        logging.info(f"Found {total_documents} new documents to process.")
        # Reset cursor after counting
        documents = art_collection.find(query)
    except Exception as e:
        logging.error(f"Error fetching documents from MongoDB: {e}")
        return

    for doc in documents[:2000]: #limiting to 2000 documents each time
        lot_link = doc.get('lot_link')
        lot_id = doc.get('lot_id')
        auction_date = doc.get('auction_date')
        iso_date = doc.get('iso_date')
        s3_url = doc.get('s3_url')
        winning_bid = doc.get('winning_bid')
        lo_est = doc.get('lo_est')
        hi_est = doc.get('hi_est')
        artist_name = doc.get('artist_name')  # Extract artist_name

        if not lot_link or not lot_id:
            logging.warning("Document missing 'lot_link' or 'lot_id'. Skipping...")
            continue

        # Fetch and parse API data
        try:
          if lot_id is None:
            logging.error(f"Lot ID is missing for lot_link: {lot_link}")
            continue
          bids = fetch_and_parse_bid_data(lot_link, int(lot_id))
        except Exception as e:
            logging.error(f"Error fetching and parsing bid data for lot_id {lot_id}: {e}")
            continue
        if not bids:
            continue

        # wait 60 seconds for every 1000 requests and insert data into MongoDB
        if len(all_data) % 1000 == 0 and len(all_data) > 0:
            logging.info("Waiting for 60 seconds to avoid rate limiting...")
            time.sleep(60)

        # Prepare the row data with MongoDB fields
        row = {
            'auction_house': doc.get('auction_house'),
            'lot_link': lot_link,
            'lot_id': int(lot_id),
            'auction_date': auction_date,
            'iso_date': iso_date,
            's3_url': s3_url,
            'winning_bid': winning_bid,
            'lo_est': lo_est,
            'hi_est': hi_est,
            'artist_name': artist_name  # Include artist_name
        }

        # Add bid columns dynamically (bid_1, bid_2, ...)

        for bid in bids:
            bid_number = bid['Bid Number']
            amount_usd = bid['Amount($)']
            # Include Amount($) as an example. You can expand this to include other bid details.
            row[f'bid_{bid_number}_usd'] = amount_usd
            row[f'bid_{bid_number}_rs'] = bid['Amount(Rs)']
            row[f'bid_{bid_number}_name'] = bid['Nickname']
            row[f'bid_{bid_number}_type'] = bid['Type']
            row[f'bid_{bid_number}_datetime'] = bid['Date & Time(US EST)']

        all_data.append(row)

    if all_data:
        df = pd.DataFrame(all_data)

        # Handle dynamic bid columns by sorting and renaming them
        bid_columns_usd = sorted([col for col in df.columns if re.match(r'bid_\d+_usd', col)],
                                  key=lambda x: int(re.search(r'bid_(\d+)_usd', x).group(1)))
        bid_columns_rs = sorted([col for col in df.columns if re.match(r'bid_\d+_rs', col)],
                                key=lambda x: int(re.search(r'bid_(\d+)_rs', x).group(1)))
        bid_columns_type = sorted([col for col in df.columns if re.match(r'bid_\d+_type', col)],
                                  key=lambda x: int(re.search(r'bid_(\d+)_type', x).group(1)))
        bid_columns_datetime = sorted([col for col in df.columns if re.match(r'bid_\d+_datetime', col)],
                                      key=lambda x: int(re.search(r'bid_(\d+)_datetime', x).group(1)))



        # Combine all bid columns in order
        bid_numbers = set(int(re.search(r'bid_(\d+)_usd', col).group(1)) for col in bid_columns_usd)
        max_bid_number = max(bid_numbers) if bid_numbers else 1

        bid_columns = []
        for i in range(1, max_bid_number + 1):
            bid_columns.extend([
                f'bid_{i}_usd',
                f'bid_{i}_rs',
                f'bid_{i}_name',
                f'bid_{i}_type',
                f'bid_{i}_datetime'
            ])

        # Reorder DataFrame columns
        fixed_columns = [
            'auction_house',
            'lot_link',
            'lot_id',
            'auction_date',
            'iso_date',
            's3_url',
            'winning_bid',
            'lo_est',
            'hi_est',
            'artist_name'  # Ensure artist_name is included
        ]
        df = df[fixed_columns + bid_columns]

        # Display the DataFrame
        logging.info("Bid data fetched successfully!")
        df=transform_bid_data(df)

        # Provide a download button for CSV
        csv = df.to_csv(index=False).encode('utf-8')

        # Insert data into 'saffron_bid_data' collection
        try:
            # Prepare new data for insertion
            new_data = df.to_dict('records')

            if new_data:
                saffron_bid_data_collection.insert_many(new_data)
                logging.info(f"Inserted {len(new_data)} new documents into 'saffron_bid_data' collection.")
            else:
                logging.info("No new data to insert into 'saffron_bid_data' collection.")
        except Exception as e:
            logging.error(f"Error inserting data into MongoDB: {e}")

    else:
        logging.info("No new bid data to process.")

    logging.info("Bid data fetching process completed.")

if __name__ == "__main__":
    bid_main()
