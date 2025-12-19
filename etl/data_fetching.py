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


def get_latest_date_in_db():
    try:
        logger.info(
            "Connecting to the PostgreSQL database to get the latest date...")
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        cursor = connection.cursor()
        query = "SELECT MAX(trading_date) FROM market_data;"
        cursor.execute(query)
        result = cursor.fetchone()
        latest_date = result[0] if result[0] is not None else '01/01/2016'
        logger.info(f"Latest date in DB: {latest_date}")
        return latest_date.strftime("%d/%m/%Y")
    except Exception as e:
        logger.error(f"Error while fetching latest date from DB: {e}")
        return '01/01/2016'


def update_db(start_date='01/01/2016', end_date='01/01/2021'):
    try:
        logger.info("Fetching historical crypto data from investpy...")
        df = investpy.get_crypto_historical_data(
            crypto='bitcoin', from_date=start_date, to_date=end_date)
        return df
    except Exception as e:
        logger.error(f"Error while fetching data from investpy: {e}")
        return None


if __name__ == "__main__":
    df = update_db()
    if df is not None:
        print(df.head())
