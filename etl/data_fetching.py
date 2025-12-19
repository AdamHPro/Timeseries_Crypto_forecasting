import investpy
import os
import logging
import psycopg2

logger = logging.getLogger(__name__)

# This must be put in the "main.py" file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

DB_HOST = os.getenv("DB_HOST", "localhost")

DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")  # Default user

DB_PASS = os.getenv("DB_PASS", "postgres")
DB_PORT = os.getenv("DB_PORT", "5433")


def update_db():
    try:
        logger.info("Fetching historical crypto data from investpy...")
        df = investpy.get_crypto_historical_data(
            crypto='bitcoin', from_date='01/01/2016', to_date='01/01/2021')
        return df
    except Exception as e:
        logger.error(f"Error while fetching data from investpy: {e}")
        return None


if __name__ == "__main__":
    df = update_db()
    if df is not None:
        print(df.head())
