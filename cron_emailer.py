import os
import smtplib
import logging
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pymongo import MongoClient
from dotenv import load_dotenv
import time
from copy import deepcopy

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('email_distribution.log'),
        logging.StreamHandler()
    ]
)

def send_links_to_subscribers():
    """Send links from links.txt to validated subscribers with error handling"""
    try:
        logging.info("Starting link distribution to subscribers")

        # MongoDB connection with error handling
        try:
            client = MongoClient(os.getenv("MONGO_URI"))
            db = client[os.getenv("DATABASE_NAME")]
            collection = db[os.getenv("MONGO_EMAIL_COLLECTION")]
        except Exception as e:
            logging.critical(f"MongoDB connection failed: {str(e)}")
            return

        # Email collection with validation
        try:
            subscribers = list(collection.find({}, {'email': 1, '_id': 0}))
            valid_emails = [
                sub['email'].strip().lower()
                for sub in subscribers
                if sub.get('email') and '@' in sub['email']
            ]
            if not valid_emails:
                logging.warning("No valid subscriber emails found")
                return
            logging.info(f"Found {len(valid_emails)} valid subscribers")
        except KeyError as e:
            logging.error(f"Missing email field in documents: {str(e)}")
            return

        # Read links from links.txt
        try:
            with open('links.txt', 'r', encoding='utf-8') as file:
                links_text = file.read()
                if not links_text.strip():
                    logging.error("links.txt is empty or missing")
                    return
        except FileNotFoundError:
            logging.error("links.txt file not found")
            return

        # Create base email message
        base_msg = MIMEMultipart()
        base_msg['From'] = os.getenv("EMAIL_USER")
        base_msg['Subject'] = f"Latest Links - {datetime.now().strftime('%Y-%m-%d')}"

        # Email body
        body_text = links_text
        base_msg.attach(MIMEText(body_text, 'plain'))

        # SMTP connection
        try:
            with smtplib.SMTP(os.getenv("SMTP_SERVER"), os.getenv("SMTP_PORT")) as server:
                server.starttls()
                server.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASSWORD"))

                for index, email in enumerate(valid_emails):
                    try:
                        # Clone the base message for each recipient
                        msg = deepcopy(base_msg)
                        msg['To'] = email

                        server.sendmail(
                            os.getenv("EMAIL_USER"),
                            email,
                            msg.as_string()
                        )
                        logging.info(f"Sent to {email} ({index+1}/{len(valid_emails)})")

                        # Rate limiting
                        if index < len(valid_emails) - 1:
                            time.sleep(1)  # 1 second between emails

                    except Exception as e:
                        logging.error(f"Failed to send to {email}: {str(e)}")

        except Exception as e:
            logging.critical(f"SMTP connection failed: {str(e)}")
            return

        logging.info(f"Successfully sent emails to {len(valid_emails)} subscribers")

    except Exception as e:
        logging.critical(f"Critical failure: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    send_links_to_subscribers()