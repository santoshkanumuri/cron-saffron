import requests
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import time
import re
import dotenv
import logging
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup as BS
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from PIL import Image, ImageStat
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import chromedriver_autoinstaller
from webdriver_manager.chrome import ChromeDriverManager
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
# Load environment variables
dotenv.load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("saffron_scraper.log"),
        logging.StreamHandler()
    ]
)

# Constants
SAFFRON_KEY = "Saffron Art"
SAFFRON_ART_SCRAPE_PATH = 'saffron_art_scrape.csv'
NEXT_PAGE_IDS = ["ContentPlaceHolder1_Next", "ContentPlaceHolder1_lnkNext"]

# MongoDB configuration
MONGO_URI = os.getenv('MONGO_URI')
DATABASE_NAME = 'art_database'
COLLECTION_NAME = 'art_collection'

# Email configuration
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
RECIPIENT_EMAIL = os.getenv('RECIPIENT_EMAIL')

def main():
    """Main function to run scraping and email sending"""
    try:
        logging.info("Starting Saffron Art scraping process")

        # Connect to MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Get last scraped date
        last_auction_date = get_last_auction_collected(collection)

        # Scrape new data
        new_data = scrape_new_auctions(last_auction_date, collection)

        if new_data:
            # Save to CSV
            df = pd.DataFrame(new_data)
            csv_path = f"saffron_art_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(csv_path, index=False)
            logging.info(f"Saved new data to {csv_path}")

            # Send email with attachment
            upload_to_s3(csv_path)
            send_email(csv_path, has_data=True)
        else:
            # Send status email with no data
            send_email(has_data=False)

    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")
        send_error_email(str(e))
    finally:
        logging.info("Scraping process completed")

def upload_to_s3(file_path, bucket_name="cron-saffron", object_name="new_saffron_data.csv"):
    """
    Upload a file to an AWS S3 bucket

    :param file_path: Path to local file to upload
    :param bucket_name: Target S3 bucket name
    :param object_name: S3 object name (optional). If not specified, uses file name
    :return: True if successful, False otherwise
    """
    # If S3 object_name not specified, use file name
    if object_name is None:
        object_name = os.path.basename(file_path)

    # Initialize S3 client
    s3_client = boto3.client('s3')

    try:
        # Check if file exists locally
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist")

        # Upload the file
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"File {file_path} uploaded to s3://{bucket_name}/{object_name}")
        return True

    except FileNotFoundError as e:
        print(f"Error: {str(e)}")
        return False
    except NoCredentialsError:
        print("Error: AWS credentials not found")
        return False
    except ClientError as e:
        print(f"AWS Client Error: {str(e)}")
        return False
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        return False

def send_email(csv_path=None, has_data=False, error_msg=None):
    """Send email with CSV attachment or status message"""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = RECIPIENT_EMAIL

    if error_msg:
        msg['Subject'] = "Saffron Scraper Error Alert"
        body = f"Scraping failed with error:\n\n{error_msg}"
    elif has_data:
        msg['Subject'] = f"Saffron Art Data - {datetime.now().strftime('%Y-%m-%d')}"
        body = "New Saffron Art data is attached and uploaded to S3 for further processing."
        # Attach CSV
        with open(csv_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(csv_path)}",
        )
        msg.attach(part)
    else:
        msg['Subject'] = "Saffron Art Data Update"
        body = "No new data found in this scraping cycle."

    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
        logging.info("Email sent successfully")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")

def send_error_email(error_msg):
    """Send error notification email"""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = RECIPIENT_EMAIL
        msg['Subject'] = "Saffron Scraper Error Alert"
        body = f"The scraping process encountered an error:\n\n{error_msg}"
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
        logging.info("Error email sent successfully")
    except Exception as e:
        logging.error(f"Failed to send error email: {str(e)}")

def initialize_driver():
    # Ensure Chrome browser is installed
    chromedriver_autoinstaller.install()

    # Set Chrome options
    options = Options()
    options.add_argument("--headless=new")  # Modern headless mode
    options.add_argument("--log-level=3")   # Only fatal errors
    options.add_argument("--disable-gpu")   # Disable GPU hardware acceleration
    options.add_argument("--no-sandbox")    # Disable sandbox for Docker/CI compatibility

    # Configure driver with automatic management
    service = Service(ChromeDriverManager().install())

    # Initialize driver
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(10)
    return driver

# Streamlit

def get_last_auction_collected(collection):
    """
    Retrieves the latest auction date from the MongoDB collection for 'Saffron Art'.
    Utilizes the 'iso_date' field, which is in ISO 8601 format.

    Returns:
        datetime: The latest auction date or datetime.min if not found.
    """
    try:
        # Retrieve the latest auction based on 'iso_date'
        last_document = collection.find_one(
            {'auction_house': SAFFRON_KEY},
            sort=[('iso_date', -1)]
        )
        if last_document and 'iso_date' in last_document:
            last_date = last_document['iso_date']
            if isinstance(last_date, str):
                try:
                    # Parse the ISO 8601 string to a datetime object
                    dt = datetime.fromisoformat(last_date)
                except ValueError:
                    logging.error(f"Date format mismatch for iso_date: {last_date}")
                    dt = datetime.min
                last_date = dt  # Keep it as datetime object
            elif isinstance(last_date, datetime):
                # Already a datetime object
                pass
            else:
                # Unrecognized format, set to minimum datetime
                last_date = datetime.min
            return last_date
        else:
            return datetime.min
    except Exception as e:
        logging.error(f"Error retrieving last auction date: {e}")
        return datetime.min

def scrape_new_auctions(last_auction_collected_date, collection):
    """
    Fetches all auctions from Saffron Art's API, filters out the already scraped ones,
    and prepares a list of new auctions to scrape.

    Args:
        last_auction_collected_date (datetime): The date of the last scraped auction.
        collection (MongoClient.collection): The MongoDB collection instance.

    Returns:
        list: A list of new auction data dictionaries.
    """
    # Get all auctions from Saffron Art website
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json; charset=utf-8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': "https://www.saffronart.com/auctions/allauctions.aspx",
        'Content-Type': 'application/json; charset=utf-8',
        'Origin': "https://www.saffronart.com",
        'Connection': 'keep-alive',
        'TE': 'Trailers',
    }

    try:
        response = requests.get('https://www.saffronart.com/Service1.svc/FetchAllSaffronAuctions/?AucType=ART', headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch auctions from Saffron Art: {e}")
        logging.error(f"Failed to fetch auctions: {e}")
        return None

    try:
        auction_json = response.json()
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON response: {e}")
        logging.error(f"JSON parsing error: {e}")
        return None

    # Validate JSON structure
    if "Events" not in auction_json:
        logging.error("JSON response does not contain 'Events' key.")
        logging.error("Missing 'Events' key in JSON response.")
        return None

    events = auction_json["Events"]

    # Ensure 'Events' is a list with at least three elements
    if not isinstance(events, list) or len(events) < 3:

        logging.error("Invalid 'Events' structure in JSON response.")
        return None

    # Ensure the third element is a list
    if not isinstance(events[2], list):

        logging.error("Third element in 'Events' is not a list.")
        return None

    auctions_data = events[2]

    if not auctions_data:

        logging.warning("No auction data found.")
        return None

    auction_df = pd.DataFrame(auctions_data)

    # Check if necessary columns exist
    required_columns = ['EventStartDate', 'EventEndDate', 'EventId']
    for col in required_columns:
        if col not in auction_df.columns:

            logging.error(f"Missing column '{col}' in auction data.")
            return None

    # Clean up the event dates from API response
    auction_df['EventStartDate'] = auction_df['EventStartDate'].str.extract(r'/Date\((\d+)-').astype(float)
    auction_df['EventEndDate'] = auction_df['EventEndDate'].str.extract(r'/Date\((\d+)-').astype(float)

    # Convert timestamps to datetime
    auction_df['s_date'] = pd.to_datetime(auction_df['EventStartDate'], unit='ms')
    auction_df['e_date'] = pd.to_datetime(auction_df['EventEndDate'], unit='ms')

    # Build the list of auctions to scrape
    auctions_to_scrape = []

    for index, row in auction_df.iterrows():
        s_date = row['s_date']
        e_date = row['e_date']

        if s_date > last_auction_collected_date and s_date < pd.Timestamp.now():
            auction_link = f"https://www.saffronart.com/auctions/PostCatalog.aspx?eid={row['EventId']}"
            auctions_to_scrape.append({
                'link': auction_link,
                's_date': s_date,
                'e_date': e_date
            })

    if not auctions_to_scrape:

        logging.info("No new auctions found.")
        return None

    # Start scraping auctions
    new_data = []

    total_auctions = len(auctions_to_scrape)
    for idx, auction in enumerate(auctions_to_scrape):
        logging.info(f"Scraping auction {idx + 1} of {total_auctions}: {auction['link']} starting on {auction['s_date'].strftime('%Y-%m-%d')}")

        auction_data = scrape_auction(auction, collection)
        if auction_data:
            new_data.extend(auction_data)

    return new_data

def generate_auction_string(auction_link: str, auction_date: str, lot_id) -> str:
    """
    Generates a concatenated string in the format 'S-{auction_date}-{eid}' and appends it to a base directory path.

    Args:
        auction_link (str): The URL of the auction containing the 'eid' parameter.
        auction_date (str): The date of the auction in 'yyyy-mm-dd' format.
        lot_id: The ID of the lot.

    Returns:
        str: The complete path string in the format 'data_files/new/saffron/S-{auction_date}-{eid}/{lot_id}.jpg'.
    """
    # Parse the URL to extract query parameters
    parsed_url = urlparse(auction_link)
    query_params = parse_qs(parsed_url.query)

    # Extract 'eid' parameter
    eid_list = query_params.get('eid')
    if not eid_list:
        raise ValueError(f"'eid' parameter not found in the auction link: {auction_link}")

    eid = eid_list[0]  # Get the first 'eid' value

    # Validate auction_date format using regular expression
    if not isinstance(auction_date, str):
        raise TypeError("auction_date must be a string in 'yyyy-mm-dd' format.")
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', auction_date):
        raise ValueError("auction_date must be in 'yyyy-mm-dd' format.")

    # Generate the concatenated string
    concatenated_str = f"{auction_date}-{eid}"

    # Define the base directory path
    base_dir = os.path.join("data_files", "neww", "saffron")

    # Ensure lot_id is a string and strip any leading/trailing whitespace
    lot_id_str = str(lot_id).strip()

    # Define the filename with .jpg extension
    filename = f"{lot_id_str}.jpg"

    # Combine base directory, concatenated string, and filename using os.path.join
    final_path = os.path.join(base_dir, concatenated_str, filename)
    print(final_path)

    return final_path

def scrape_auction(auction, collection):
    """
    Scrapes individual auction details using Selenium WebDriver.

    Args:
        auction (dict): A dictionary containing auction link and dates.
        collection (MongoClient.collection): The MongoDB collection instance.

    Returns:
        list: A list of dictionaries containing scraped lot data.
    """
    # Set up Selenium WebDriver
    try:
        driver = initialize_driver()
    except Exception as e:
        logging.error(f"Selenium WebDriver initialization error: {e}")
        return []

    auction_link = auction['link']
    try:
        driver.get(auction_link)
    except Exception as e:
        logging.error(f"Failed to load auction page: {e}")
        driver.quit()
        return []

    lot_link_list = []

    # Pagination handling
    next_page_type = check_for_page_type(driver)
    page_number = 1
    while True:
        try:
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            w_l = get_want_this_data_ids(driver)
            lot_link_list.extend(w_l)

            next_page = find_element_by_id_if_exists(driver, next_page_type)

            if not next_page or next_page.get_attribute('disabled'):
                break
            else:
                driver.execute_script("arguments[0].click();", next_page)
                driver.implicitly_wait(10)
                page_number += 1
        except Exception as e:
            logging.error(f"Error during pagination on {auction_link}: {e}")
            break

    if not lot_link_list:
        logging.warning("No lots found in the auction.")
        driver.quit()
        return []

    # Scrape lot data
    auction_data = []
    total_lots = len(lot_link_list)
    for idx, lot_link in enumerate(lot_link_list):
        if pd.isnull(lot_link):
            continue
        if not lot_link.startswith('https://www.saffronart.com/auctions/PostWork.aspx?l='):
            continue
        if not lot_link:
            logging.warning("Empty lot link. Skipping.")
            continue

        logging.info(f"Processing lot {idx + 1} of {total_lots}")
        # Process each lot
        lot_data = process_lot(lot_link, auction, collection)
        if lot_data:
            auction_data.append(lot_data)
        else:
            logging.warning(f"Failed to process lot: {lot_link}")

    driver.quit()
    return auction_data

def process_lot(lot_link, auction, collection):
    """
    Processes an individual lot and extracts data.

    Args:
        lot_link (str): The URL of the lot.
        auction (dict): The auction details.
        collection (MongoClient.collection): The MongoDB collection instance.

    Returns:
        dict: A dictionary containing the lot data.
    """
    logging.info(f"Scraping lot: {lot_link}")
    # Check if the lot already exists in MongoDB
    if collection.find_one({'lot_link': lot_link}):
        logging.info(f"Lot {lot_link} already exists in the database. Skipping.")
        return None

    lot_data = scrape_each_reg_work(lot_link)
    if lot_data:
        (
            lot_id,
            artist_name,
            dob,
            dod,
            institution,
            title,
            winning_bid,
            lo_est,
            hi_est,
            auction_name,
            category,
            style,
            provenance,
            last_provenance,
            num_provenance,
            exhibition,
            details,
            image_url
        ) = lot_data

        # Process image features
        if image_url:
            try:
                dom_color, brightness = get_img_dom_color_and_brightness(image_url)
            except Exception as e:
                logging.error(f"Error processing image for lot {lot_link}: {e}")
                dom_color = None
                brightness = None
        else:
            dom_color = None
            brightness = None

        # Clean and prepare data
        art_data = {
            "lot_id": lot_id,
            "lot_link": lot_link,
            "artist_name": artist_name,
            "dob": dob,
            "dod": dod,
            "institution": institution,
            "title": title,
            "winning_bid": winning_bid,
            "lo_est": lo_est,
            "hi_est": hi_est,
            "auction_name": auction_name,
            "auction_date": auction['s_date'].strftime('%Y-%m-%d'),
            "auction_link": auction['link'],
            "category": category,
            "style": style,
            "provenance": provenance,
            "last_provenance": last_provenance,
            "num_provenance": num_provenance,
            "exhibition": exhibition,
            "details": details,
            "image_url": image_url,
            "dom_color": dom_color,
            "brightness": brightness,
            'iso_date': auction['s_date'].isoformat(),
            'est_curr': 'USD',
        }

        # Extract integer lot_id
        int_lot_id = re.findall(r'\d+', str(lot_id))
        if int_lot_id:
            lot_id_int = int_lot_id[0]
        else:
            lot_id_int = lot_id

        art_data['none_@file'] = generate_auction_string(auction['link'], auction['s_date'].strftime('%Y-%m-%d'), lot_id_int)

        date_time = datetime.now().strftime('%Y%m%d-%H%M%S')
        art_data = clean_saffron(art_data, date_time)

        return art_data
    else:
        logging.warning(f"Failed to scrape lot: {lot_link}")
        return None

def get_want_this_data_ids(driver):
    """
    Retrieves all lot links from the auction page by finding elements with class 'WantThis'.

    Args:
        driver (webdriver): The Selenium WebDriver instance.

    Returns:
        list: A list of lot URLs.
    """
    try:
        art_tags = driver.find_elements(By.CLASS_NAME, 'WantThis')
        lot_list = []
        for lot_id in art_tags:
            work_id = lot_id.get_attribute('data-id')
            if work_id:
                lot_list.append(f"https://www.saffronart.com/auctions/PostWork.aspx?l={work_id}")
        return lot_list
    except Exception as e:
        logging.error(f"Error retrieving 'WantThis' elements: {e}")
        return []

def check_for_page_type(driver):
    """
    Determines the next page button's ID from the list of possible IDs.

    Args:
        driver (webdriver): The Selenium WebDriver instance.

    Returns:
        str or None: The ID of the next page button or None if not found.
    """
    for next_page_id in NEXT_PAGE_IDS:
        if find_element_by_id_if_exists(driver, next_page_id):
            return next_page_id
    return None

def find_element_by_id_if_exists(_driver, id_):
    try:
        return _driver.find_element(By.ID, id_)
    except:
        return None

def clean_saffron(art_data, date_time):
    """
    Cleans and processes the art data dictionary.

    Args:
        art_data (dict): The art data dictionary.
        date_time (str): The current date and time as a string.

    Returns:
        dict: The cleaned art data dictionary.
    """
    art_data['auction_house'] = SAFFRON_KEY

    try:
        art_data["winning_bid"] = str(art_data["winning_bid"]).lstrip("$").rstrip("\r").replace(",", "")
    except:
        art_data["winning_bid"] = "Not Available"

    try:
        art_data['size'] = (art_data['details'].split('|')[-2]).replace(r"\(.*\)", "")
    except:
        art_data['size'] = ""

    try:
        art_data['medium'] = art_data['details'].split('|')[-3].lower().strip()
    except:
        art_data['medium'] = ""

    try:
        art_data['date_scraped'] = datetime.strptime(date_time, '%Y%m%d-%H%M%S')
    except:
        art_data['date_scraped'] = None

    # Sold column is 1/0 based on winning bid
    try:
        art_data["sold"] = 1 if art_data.get("winning_bid") else 0
    except:
        art_data["sold"] = 0

    try:
        art_data['year_painted'] = re.search('([0-9]{4})', art_data['details']).group(1)
    except:
        art_data['year_painted'] = None

    for col in ["artist_name", "category", "style"]:
        try:
            art_data[col] = ' '.join(art_data[col].split())
        except:
            pass

    for col in ["lo_est", "hi_est"]:
        try:
            art_data[col] = int(str(art_data[col]).lstrip("$").rstrip("\r").replace(",", ""))
        except:
            art_data[col] = "Not Available"

    # Media category mapping
    media_category_list = []
    m_db_path = os.path.join(os.path.dirname(__file__), "Media_DB.csv")
    if os.path.exists(m_db_path):
        media_db = pd.read_csv(m_db_path)
        possible_mediums = media_db["Media"].tolist()
        possible_mediums.extend(["paper", "board", "canvas"])
        possible_mediums = [sub.replace(r')', '').replace(r'(', '') for sub in possible_mediums]
        s = r"[^\|]\|"
        possible_mediums = [mystring + s for mystring in possible_mediums]
        s = r"\|[^\|]+?"
        possible_mediums = [s + mystring for mystring in possible_mediums]
        medium_regex = r'|'.join(possible_mediums)
        try:
            tmp = re.search(medium_regex, art_data['details']).group(1)
            tmp = tmp.replace(r"\|", "").strip()
            art_data['medium'] = re.sub(r".*(\d+).*", "", art_data['medium'])
            if art_data["medium"] == '':
                art_data['medium'] = tmp
        except:
            pass

        media_type = art_data["medium"]
        media_category = ""
        media_type_cleaned = media_type.strip()

        filtered_df = media_db.query('Media == @media_type_cleaned')
        if media_type_cleaned.endswith("paper"):
            media_category = "1"
        elif media_type_cleaned.endswith("board") or media_type_cleaned.endswith("card") or media_type_cleaned.endswith("canvas"):
            media_category = "2"
        else:
            try:
                media_category = str(filtered_df.iloc[0]['Category'])
            except:
                media_category = ""

        art_data["medium_category"] = media_category
    else:
        art_data["medium_category"] = ""

    # Size processing
    art_data['size'] = art_data['size'].lower().strip()
    art_data['size'] = ''.join([" " if ord(i) < 32 or ord(i) > 126 else i for i in art_data["size"]]).rstrip()
    try:
        art_data['size_x'] = float(re.search(r'(\d*\.\d+|\d+)', art_data['size']).group(1))
    except:
        art_data['size_x'] = 0

    try:
        art_data['size_y'] = float(re.search(r'x (\d*\.\d+|\d+)', art_data['size']).group(1))
    except:
        art_data['size_y'] = 0

    art_data['size_z'] = 0
    art_data['area'] = art_data['size_y'] * art_data['size_x']

    try:
        art_data["lot_id"] = re.search(r'(\d*\.\d+|\d+)', art_data['lot_id']).group(1)
    except:
        pass

    # Signed
    watchlist = ['signed', 'initial', 'inscribed']
    art_data['signed'] = int(bool(re.search('|'.join(watchlist), art_data['details']))) if art_data['details'] else 0

    # Multiple items
    try:
        watchlist = ["Diptych", 'Triptych', 'set of', '\\| b\\)']
        art_data['multi_item'] = int(bool(re.search('|'.join(watchlist), art_data['details'])))
        art_data['num_items'] = art_data['details'].count(r'\| [a-z]\)') + 1
    except:
        art_data['multi_item'] = 0
        art_data['num_items'] = 1

    return art_data

def get_img_dom_color_and_brightness(url):
    """
    Calculates the dominant color and brightness of an image from a URL.

    Args:
        url (str): The image URL.

    Returns:
        tuple: A tuple containing the dominant color in hex format and the brightness value.
    """
    try:
        with Image.open(requests.get(url, stream=True, timeout=10).raw) as img:
            img.thumbnail((100, 100))
            # Reduce colors
            paletted = img.convert('P', palette=Image.Palette.ADAPTIVE, colors=5)
            palette = paletted.getpalette()
            color_counts = sorted(paletted.getcolors(), reverse=True)
            palette_index = color_counts[0][1]
            dominant_color = palette[palette_index * 3:palette_index * 3 + 3]

            im = img.convert('HSV')
            brightness = ImageStat.Stat(im).mean[2]

            return rgb_to_hex(tuple(dominant_color)), str(brightness)
    except Exception as e:
        logging.error(f"Error processing image {url}: {e}")
        return None, None

def rgb_to_hex(rgb):
    return '#%02x%02x%02x' % rgb

def scrape_each_reg_work(reg_work_link):
    """
    Scrapes the details of each regular work (lot) from the Saffron Art website.

    Args:
        reg_work_link (str): The URL of the lot.

    Returns:
        tuple: A tuple containing all the extracted data for the lot.
    """
    while True:
        try:
            response = requests.get(reg_work_link, cookies={'UserPref': 'ps=20'}, timeout=10)
            response.raise_for_status()
            reg_work_soup = BS(response.text, 'lxml')
            lot_id = reg_work_soup.find_all("div", {"class": "clearfix artworkImageOptions"})[-1].get_text()
            break
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e}")
            time.sleep(5)
            continue
        except Exception as e:
            logging.error(f"Error parsing lot page: {e}")
            return None

    try:
        auction_info = reg_work_soup.find('div', class_='artworkDetails').p.strong.text
        auction_name = auction_info.split('\n')[1].strip()
        ad = re.sub(r'-.*?\s', ' ', str(auction_info.split('\n')[3].strip()))
        auction_date = datetime.strptime(ad, "%d %B %Y")
    except Exception as e:
        logging.error(f"Error extracting auction info: {e}")
        return None

    try:
        estimate_text = reg_work_soup.find('label', id='ContentPlaceHolder1_lblEstimates').text.split('\n')
        lo_est, hi_est = get_estimates(estimate_text)
    except AttributeError:
        lo_est, hi_est = None, None

    try:
        winning_bid_text = reg_work_soup.find('b', class_='wining-text').find_next('strong').text.split('\n')
        winning_bid = get_winning_bid(winning_bid_text)
    except Exception as e:
        winning_bid = None

    try:
        artist_name = reg_work_soup.find('a', id='ContentPlaceHolder1_AboutWork1__ArtistName').text.strip()
        artist_name = artist_name.replace("  ", " ")
    except:
        artist_name = "NONE"

    # For the purpose of this script, we'll assume artist data is not available
    DOB = None
    DOD = None
    institution = None

    try:
        title = reg_work_soup.find('span', id='ContentPlaceHolder1_AboutWork1_sn_Workdetails').i.text
    except:
        title = "NONE"

    try:
        image_url = reg_work_soup.find(id='ContentPlaceHolder1_WorkDetails1__Image')['src']
    except:
        image_url = None

    try:
      details_soup = reg_work_soup.find('span', id='ContentPlaceHolder1_AboutWork1_sn_Workdetails').parent
      details_list = [element.string for element in details_soup.find_all(string=True, recursive=False)]
      details = get_details(details_list)
    except Exception as e:
      details = ""
      print(f"An error occurred: {e}")


    try:
        provenance = reg_work_soup.find('p', id='ContentPlaceHolder1_AboutWork1__Provenance').get_text(" | ")
        provenance = provenance.replace("PROVENANCE | ", "")
        last_provenance = provenance.split(" | ")[-1]
        num_provenance = provenance.count(" | ") + 1
    except AttributeError:
        provenance = None
        last_provenance = ""
        num_provenance = 0

    try:
        exhibition = reg_work_soup.find('p', id='ContentPlaceHolder1_AboutWork1__PublishingDesc').get_text()
    except AttributeError:
        exhibition = None

    try:
        category_style = reg_work_soup.find('a', id='ContentPlaceHolder1_AboutWork1_TellAFriendLink').parent.find_previous_sibling('p').text
    except:
        category_style = "NONE"

    try:
        category = category_style.split('Category: ')[1].split('\n')[0].strip()
    except IndexError:
        category = None

    try:
        style = category_style.split('Style: ')[1].split('\n')[0].strip()
    except IndexError:
        style = None

    return (
        lot_id, artist_name, DOB, DOD, institution, title, winning_bid, lo_est, hi_est, auction_name,
        category, style, provenance, last_provenance, num_provenance, exhibition, details, image_url)

def connect(link, query=None):
    """
    Connects to the given URL and retrieves the HTML content.

    Args:
        link (str): The URL to connect to.
        query (dict, optional): Additional query parameters.

    Returns:
        str: The HTML content of the page.
    """
    while True:
        try:
            return requests.get(link, params=query, cookies={'UserPref': 'ps=20'}, timeout=10).text
        except requests.exceptions.RequestException as connecting_error:
            logging.error(f"Connection error: {connecting_error}")
            time.sleep(5)
            continue

def get_estimates(estimate_text):
    """
    Extracts the low and high estimates from the estimate text.

    Args:
        estimate_text (list): The list of estimate strings.

    Returns:
        tuple: A tuple containing low estimate and high estimate.
    """
    try:
        lo_est = estimate_text[1].split(' - ')[0].split('$')[1]
        hi_est = estimate_text[1].split(' - ')[1]
    except IndexError:
        lo_est = estimate_text[3].split(' - ')[0].split('$')[1]
        hi_est = estimate_text[3].split(' - ')[1]

    return (lo_est, hi_est)

def get_winning_bid(winning_bid_text):
    """
    Extracts the winning bid from the text.

    Args:
        winning_bid_text (list): The list of winning bid strings.

    Returns:
        str: The winning bid amount.
    """
    price = [i for i in winning_bid_text if i.strip().startswith('$')][0]
    price = price.replace(",", "").replace("$", "").strip()
    return price

def get_details(details_list):
    """
    Compiles the artwork details from the list.

    Args:
        details_list (list): The list of detail strings.

    Returns:
        str: The compiled details string.
    """
    full_details = ''

    for detail in details_list[:-1]:
        detail = ' '.join(' '.join(detail.split('\n')).split())
        full_details += detail + ' | '

    return full_details

if __name__ == "__main__":
    main()
