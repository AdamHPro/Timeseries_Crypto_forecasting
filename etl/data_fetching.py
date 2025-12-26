import yfinance as yf
import os
import logging
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# This must be put in the "main.py" file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

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


def pull_data_from_yfinance(start_date, end_date):
    try:
        logger.info(
            f"Fetching historical crypto data from yfinance from {start_date} to {end_date}...")
        df = yf.download("BTC-USD", start=start_date,
                         end=end_date, progress=False)
        return df
    except Exception as e:
        logger.error(f"Error while fetching data from yfinance: {e}")
        return None


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
        cursor.close()
        connection.close()
        return latest_date.strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"Error while fetching latest date from DB: {e}")
        return '2016-01-01'


def update_db(start_date=get_latest_date_in_db(), end_date=(datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")):
    df = pull_data_from_yfinance(start_date, end_date)
    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        cursor = connection.cursor()
        df_clean = df.reset_index()
        data_list = [tuple(x) for x in df_clean.to_numpy()]
        logger.info("Inserting new data into the database...")
        query = """
        INSERT INTO market_data (
                trading_date, 
                open_price, 
                high_price, 
                low_price, 
                close_price, 
                volume
            )
        VALUES %s
        ON CONFLICT (trading_date) 
        DO UPDATE SET 
            trading_date = EXCLUDED.trading_date;
        """
        extras.execute_values(cursor, query, data_list)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info(
            f"Database update complete. Added {len(data_list)} records.")

    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        return None


if __name__ == "__main__":
    df = update_db()
    if df is not None:
        print(df.head())
