import os
import time
import re
import logging
import json
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
import requests
import pandas as pd
import numpy as np
from pymongo import MongoClient
from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from dotenv import load_dotenv
from PIL import Image, ImageStat

# Initialize environment and logging
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

firefox_path = "/usr/bin/firefox"  # Change this if Firefox is installed elsewhere
geckodriver_path = "/home/ubuntu/saffron-art-scraper/cron-saffron/geckodriver"

class SaffronArtScraper:
    def __init__(self):
        self.SAFFRON_KEY = "Saffron Art"
        self.NEXT_PAGE_IDS = ["ContentPlaceHolder1_Next", "ContentPlaceHolder1_lnkNext"]
        self.client = None
        self.collection = None
        self.driver = None
        self.setup_mongo()
        self.setup_driver()

    def setup_mongo(self):
        """Initialize MongoDB connection with retries"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.client = MongoClient(os.getenv('MONGO_URI'), serverSelectionTimeoutMS=5000)
                self.client.server_info()  # Test connection
                db = self.client[os.getenv('DATABASE_NAME', 'art_database')]
                self.collection = db[os.getenv('COLLECTION_NAME', 'art_collection')]
                logging.info("Successfully connected to MongoDB")
                return
            except Exception as e:
                logging.error(f"MongoDB connection failed (attempt {attempt+1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def setup_driver(self):
        """Initialize Chrome WebDriver"""
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        import chromedriver_autoinstaller
        
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Auto-install chromedriver
        chromedriver_path = chromedriver_autoinstaller.install()
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.driver = webdriver.Chrome(
                    service=Service(chromedriver_path),
                    options=options
                )
                self.driver.implicitly_wait(10)
                logging.info("Chrome WebDriver initialized successfully")
                return
            except Exception as e:
                logging.error(f"Chrome initialization failed (attempt {attempt+1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def get_last_auction_date(self):
        """Get last scraped auction date from MongoDB"""
        try:
            last_document = self.collection.find_one(
                {'auction_house': self.SAFFRON_KEY},
                sort=[('iso_date', -1)]
            )
            if last_document and 'iso_date' in last_document:
                return datetime.fromisoformat(last_document['iso_date'])
            return datetime.min
        except Exception as e:
            logging.error(f"Error getting last auction date: {e}")
            return datetime.min

    def scrape_and_save(self):
        """Main scraping workflow"""
        try:
            last_date = self.get_last_auction_date()
            new_data = self.scrape_new_auctions(last_date)

            if new_data:
                filename = self.save_to_csv(new_data)
                self.send_email_with_attachment(filename)
                logging.info(f"Scraping completed successfully. File: {filename}")
            else:
                logging.info("No new data found")

        except Exception as e:
            logging.error(f"Scraping failed: {e}")
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logging.error(f"Error closing WebDriver: {e}")
        if self.client:
            try:
                self.client.close()
            except Exception as e:
                logging.error(f"Error closing MongoDB connection: {e}")

    def scrape_new_auctions(self, last_auction_collected_date):
        """Scrape new auctions since last collected date"""
        logging.info("Fetching auction data from Saffron Art API")
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/json; charset=utf-8',
                'Referer': "https://www.saffronart.com/auctions/allauctions.aspx",
            }
            response = requests.get(
                'https://www.saffronart.com/Service1.svc/FetchAllSaffronAuctions/?AucType=ART',
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            auction_json = response.json()
        except Exception as e:
            logging.error(f"Failed to fetch auctions: {e}")
            return []

        try:
            events = auction_json["Events"]
            if len(events) < 3 or not isinstance(events[2], list):
                logging.error("Invalid auction data structure")
                return []
            auctions_data = events[2]
        except KeyError as e:
            logging.error(f"Missing key in auction data: {e}")
            return []

        auction_df = pd.DataFrame(auctions_data)
        required_columns = ['EventStartDate', 'EventEndDate', 'EventId']
        if not all(col in auction_df.columns for col in required_columns):
            logging.error("Missing required columns in auction data")
            return []

        # Process dates
        auction_df['EventStartDate'] = auction_df['EventStartDate'].str.extract(r'/Date\((\d+)-').astype(float)
        auction_df['EventEndDate'] = auction_df['EventEndDate'].str.extract(r'/Date\((\d+)-').astype(float)
        auction_df['s_date'] = pd.to_datetime(auction_df['EventStartDate'], unit='ms')
        auction_df['e_date'] = pd.to_datetime(auction_df['EventEndDate'], unit='ms')

        # Filter new auctions
        now = pd.Timestamp.now()
        auctions_to_scrape = []
        for _, row in auction_df.iterrows():
            if row['s_date'] > last_auction_collected_date and row['s_date'] < now:
                auction_link = f"https://www.saffronart.com/auctions/PostCatalog.aspx?eid={row['EventId']}"
                auctions_to_scrape.append({
                    'link': auction_link,
                    's_date': row['s_date'],
                    'e_date': row['e_date']
                })

        if not auctions_to_scrape:
            logging.info("No new auctions to scrape")
            return []

        # Scrape each auction
        new_data = []
        total_auctions = len(auctions_to_scrape)
        for idx, auction in enumerate(auctions_to_scrape):
            try:
                logging.info(f"Processing auction {idx+1}/{total_auctions}")
                auction_data = self.scrape_auction(auction)
                if auction_data:
                    new_data.extend(auction_data)
            except Exception as e:
                logging.error(f"Failed to process auction {auction['link']}: {e}")
                continue

        return new_data

    def scrape_auction(self, auction):
        """Scrape individual auction details"""
        logging.info(f"Scraping auction: {auction['link']}")
        try:
            self.driver.get(auction['link'])
            time.sleep(2)  # Allow page load
        except Exception as e:
            logging.error(f"Failed to load auction page: {e}")
            return []

        lot_links = []
        next_page_id = self.check_for_page_type()
        while True:
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                current_links = self.get_want_this_data_ids()
                lot_links.extend(current_links)

                next_button = self.find_element_by_id(next_page_id)
                if not next_button or next_button.get_attribute('disabled'):
                    break

                next_button.click()
                time.sleep(2)
            except Exception as e:
                logging.error(f"Pagination error: {e}")
                break

        if not lot_links:
            logging.warning("No lots found in auction")
            return []

        # Process lots
        auction_data = []
        for lot_link in lot_links:
            if not lot_link.startswith('https://www.saffronart.com/auctions/PostWork.aspx?l='):
                continue
            try:
                lot_data = self.process_lot(lot_link, auction)
                if lot_data:
                    auction_data.append(lot_data)
            except Exception as e:
                logging.error(f"Failed to process lot {lot_link}: {e}")
                continue

        return auction_data

    def process_lot(self, lot_link, auction):
        """Process individual lot"""
        if self.collection.find_one({'lot_link': lot_link}):
            logging.info(f"Skipping existing lot: {lot_link}")
            return None

        lot_data = self.scrape_each_reg_work(lot_link)
        if not lot_data:
            return None

        (lot_id, artist_name, dob, dod, institution, title, winning_bid, lo_est, hi_est,
         auction_name, category, style, provenance, last_provenance, num_provenance,
         exhibition, details, image_url) = lot_data

        # Process image features
        dom_color = brightness = None
        if image_url:
            try:
                dom_color, brightness = self.get_img_dom_color_and_brightness(image_url)
            except Exception as e:
                logging.error(f"Image processing failed: {e}")

        # Prepare data structure
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
            'est_cur': 'USD',
            'auction_house': self.SAFFRON_KEY
        }

        # Generate file path
        try:
            int_lot_id = re.findall(r'\d+', str(lot_id))[0]
            art_data['none_@file'] = self.generate_auction_string(
                auction['link'],
                auction['s_date'].strftime('%Y-%m-%d'),
                int_lot_id
            )
        except Exception as e:
            logging.error(f"Failed to generate file path: {e}")

        # Clean and enhance data
        return self.clean_saffron(art_data)

    def generate_auction_string(self, auction_link, auction_date, lot_id):
        """Generate file path string"""
        parsed_url = urlparse(auction_link)
        query_params = parse_qs(parsed_url.query)
        eid = query_params.get('eid', [''])[0]
        return os.path.join("data_files", "neww", "saffron", f"{auction_date}-{eid}", f"{lot_id}.jpg")

    def get_want_this_data_ids(self):
        """Get lot links from current page"""
        try:
            elements = self.driver.find_elements(By.CLASS_NAME, 'WantThis')
            return [
                f"https://www.saffronart.com/auctions/PostWork.aspx?l={el.get_attribute('data-id')}"
                for el in elements
            ]
        except Exception as e:
            logging.error(f"Error getting lot links: {e}")
            return []

    def check_for_page_type(self):
        """Find next page button"""
        for page_id in self.NEXT_PAGE_IDS:
            if self.find_element_by_id(page_id):
                return page_id
        return None

    def find_element_by_id(self, element_id):
        """Safe element finder"""
        try:
            return self.driver.find_element(By.ID, element_id)
        except:
            return None

    def scrape_each_reg_work(self, reg_work_link):
        """Scrape individual lot details"""
        try:
            response = requests.get(reg_work_link, timeout=30)
            response.raise_for_status()
            soup = BS(response.text, 'lxml')
        except Exception as e:
            logging.error(f"Failed to fetch lot page: {e}")
            return None

        try:
            # Extract basic info
            lot_id = soup.find("div", {"class": "clearfix artworkImageOptions"}).text.strip()
            auction_info = soup.find('div', class_='artworkDetails').p.strong.text
            auction_name = auction_info.split('\n')[1].strip()

            # Extract estimates
            estimate_text = soup.find('label', id='ContentPlaceHolder1_lblEstimates').text.split('\n')
            lo_est = hi_est = None
            if len(estimate_text) > 1:
                estimates = estimate_text[1].split(' - ')
                lo_est = estimates[0].replace('$', '').replace(',', '').strip()
                hi_est = estimates[1].replace('$', '').replace(',', '').strip()

            # Extract winning bid
            winning_bid = None
            winning_element = soup.find('b', class_='wining-text')
            if winning_element:
                winning_bid = winning_element.find_next('strong').text.replace('$', '').replace(',', '').strip()

            # Artist info
            artist_name = soup.find('a', id='ContentPlaceHolder1_AboutWork1__ArtistName').text.strip()

            # Image URL
            image_url = soup.find(id='ContentPlaceHolder1_WorkDetails1__Image')['src']

            # Details
            details_soup = soup.find('span', id='ContentPlaceHolder1_AboutWork1_sn_Workdetails')
            details = ' | '.join([elem.strip() for elem in details_soup.stripped_strings])

            # Provenance
            provenance = soup.find('p', id='ContentPlaceHolder1_AboutWork1__Provenance')
            provenance_text = provenance.get_text(" | ", strip=True).replace("PROVENANCE | ", "") if provenance else ""
            provenance_parts = provenance_text.split(" | ") if provenance_text else []
            last_provenance = provenance_parts[-1] if provenance_parts else ""
            num_provenance = len(provenance_parts)

            # Category and style
            category_style = soup.find('a', id='ContentPlaceHolder1_AboutWork1_TellAFriendLink').find_previous('p').text
            category = style = None
            if 'Category:' in category_style:
                category = category_style.split('Category: ')[1].split('\n')[0].strip()
            if 'Style:' in category_style:
                style = category_style.split('Style: ')[1].split('\n')[0].strip()

            return (
                lot_id, artist_name, None, None, None, None, winning_bid,
                lo_est, hi_est, auction_name, category, style, provenance_text,
                last_provenance, num_provenance, None, details, image_url
            )

        except Exception as e:
            logging.error(f"Error parsing lot page: {e}")
            return None

    def clean_saffron(self, art_data):
        """Clean and enhance scraped data"""
        # Price cleaning
        for field in ['winning_bid', 'lo_est', 'hi_est']:
            try:
                art_data[field] = float(str(art_data[field]).replace('$', '').replace(',', ''))
            except:
                art_data[field] = None

        # Date handling
        art_data['date_scraped'] = datetime.now().isoformat()

        # Size parsing
        try:
            size_str = art_data['details'].split('|')[-2].strip()
            dimensions = [float(d) for d in re.findall(r'\d+\.?\d*', size_str)]
            art_data['size_x'] = dimensions[0] if len(dimensions) > 0 else 0
            art_data['size_y'] = dimensions[1] if len(dimensions) > 1 else 0
            art_data['area'] = art_data['size_x'] * art_data['size_y']
        except:
            art_data.update({'size_x': 0, 'size_y': 0, 'area': 0})

        # Medium detection
        try:
            medium = art_data['details'].split('|')[-3].strip().lower()
            art_data['medium'] = re.sub(r'\d+', '', medium).strip()
        except:
            art_data['medium'] = ''

        return art_data

    def get_img_dom_color_and_brightness(self, url):
        """Analyze image characteristics"""
        try:
            with Image.open(requests.get(url, stream=True, timeout=10).raw) as img:
                img.thumbnail((100, 100))
                paletted = img.convert('P', palette=Image.Palette.ADAPTIVE, colors=5)
                palette = paletted.getpalette()
                color_counts = sorted(paletted.getcolors(), reverse=True)
                dominant = palette[color_counts[0][1]*3:color_counts[0][1]*3+3]

                hsv_img = img.convert('HSV')
                brightness = ImageStat.Stat(hsv_img).mean[2]

                return f"#{dominant[0]:02x}{dominant[1]:02x}{dominant[2]:02x}", brightness
        except Exception as e:
            logging.error(f"Image processing failed: {e}")
            return None, None

    def save_to_csv(self, data):
        """Save data to timestamped CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"saffron_art_{timestamp}.csv"
        try:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False)
            logging.info(f"Saved {len(data)} records to {filename}")
            return filename
        except Exception as e:
            logging.error(f"CSV save failed: {e}")
            raise

    def send_email_with_attachment(self, filename):
        """Send email with CSV attachment"""
        try:
            msg = MIMEMultipart()
            msg['From'] = os.getenv('EMAIL_USER')
            msg['To'] = os.getenv('RECIPIENT_EMAIL')
            msg['Subject'] = f"Saffron Art Data - {datetime.now().strftime('%Y-%m-%d')}"

            body = f"Attached is the latest Saffron Art data with {len(pd.read_csv(filename))} records."
            msg.attach(MIMEText(body, 'plain'))

            with open(filename, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
            msg.attach(part)

            with smtplib.SMTP(os.getenv('SMTP_SERVER'), os.getenv('SMTP_PORT', 587)) as server:
                server.starttls()
                server.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASSWORD'))
                server.send_message(msg)

            logging.info("Email sent successfully")
        except Exception as e:
            logging.error(f"Email failed: {e}")
            raise

if __name__ == "__main__":
    scraper = SaffronArtScraper()
    scraper.scrape_and_save()
