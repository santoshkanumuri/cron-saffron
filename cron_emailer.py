import os
import smtplib
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
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

def send_csvs_to_subscribers():
    """Send CSV files to validated subscribers with error handling"""
    try:
        logging.info("Starting CSV distribution to subscribers")

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

        # CSV file validation
        csv_files = [
            './files/similarities.csv',
            './files/bid_data.csv',
            './files/transformed_bid_data.csv'
        ]
        missing_files = [f for f in csv_files if not os.path.exists(f)]
        if missing_files:
            logging.error(f"Missing CSV files: {', '.join(missing_files)}")
            return

        # Create base email message
        base_msg = MIMEMultipart()
        base_msg['From'] = os.getenv("EMAIL_USER")
        base_msg['Subject'] = f"Latest Art Market Data - {datetime.now().strftime('%Y-%m-%d')}"

        # Email body
        body_text = """Attached are the latest art market datasets:
        1. similarities.csv - Artist similarity analysis
        2. bid_data.csv - Raw auction bid data
        3. transformed_bid_data.csv - Processed bid transactions
        """
        base_msg.attach(MIMEText(body_text, 'plain'))

        # Attach files to base message
        for csv_file in csv_files:
            try:
                with open(csv_file, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f"attachment; filename={os.path.basename(csv_file)}",
                    )
                    base_msg.attach(part)
            except Exception as e:
                logging.error(f"Failed to attach {csv_file}: {str(e)}")
                return

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
    send_csvs_to_subscribers()