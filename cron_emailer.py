import os
import smtplib
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pymongo import MongoClient



def send_csvs_to_subscribers():
    """Send all 3 CSV files to subscribers in MongoDB email collection"""
    try:
        logging.info("Starting CSV distribution to subscribers")

        # Connect to MongoDB
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("DB_NAME")]
        collection = db[os.getenv("MONGO_EMAIL_COLLECTION")]

        # Get all subscriber emails
        emails = [doc['email'] for doc in collection.find({}, {'email': 1})]
        if not emails:
            logging.warning("No subscriber emails found in collection")
            return

        logging.info(f"Found {len(emails)} subscribers to notify")

        # Validate CSV files exist
        csv_files = [
            'similarities.csv',
            'bid_data.csv',
            'transformed_bid_data.csv'
        ]

        missing_files = [f for f in csv_files if not os.path.exists(f)]
        if missing_files:
            logging.error(f"Missing CSV files: {', '.join(missing_files)}")
            return

        # Create email message
        msg = MIMEMultipart()
        msg['From'] = os.getenv("EMAIL_USER")
        msg['Subject'] = f"Latest Art Market Data - {datetime.now().strftime('%Y-%m-%d')}"

        body = MIMEText("""
            Attached are the latest art market datasets:\n
            1. similarities.csv - Artist similarity analysis\n
            2. bid_data.csv - Raw auction bid data\n
            3. transformed_bid_data.csv - Processed bid transactions\n
            """)
        msg.attach(body)

        # Attach all CSV files
        for csv_file in csv_files:
            with open(csv_file, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={csv_file}",
            )
            msg.attach(part)

        # Send emails
        with smtplib.SMTP(os.getenv("SMTP_SERVER"), os.getenv("SMTP_PORT")) as server:
            server.starttls()
            server.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASSWORD"))

            for email in emails:
                try:
                    msg.replace_header('To', email)
                    server.sendmail(os.getenv("EMAIL_USER"), email, msg.as_string())
                    logging.info(f"Email successfully sent to {email}")
                except Exception as e:
                    logging.error(f"Failed to send to {email}: {str(e)}")

        logging.info("Completed CSV distribution to subscribers")

    except Exception as e:
        logging.critical(f"Failed to send subscriber emails: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    send_csvs_to_subscribers()