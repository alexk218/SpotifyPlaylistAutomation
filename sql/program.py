# Database connection function
import logging
import os

from dotenv import load_dotenv

from sql.helpers.db_helper import clear_db, insert_db

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

logging.basicConfig(filename='spotify_script.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == "__main__":
    clear_db()
    insert_db()
