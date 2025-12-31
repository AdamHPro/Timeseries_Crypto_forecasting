from datetime import datetime
import logging
import psycopg2
from psycopg2 import extras
import os
from data_fetching import pull_data_from_yfinance, save_to_parquet


logger = logging.getLogger(__name__)


now = datetime.now()
formatted_date = now.strftime("%Y-%m-%d")

# Configuration for database connection
# Usually localhost if Docker maps the port
DB_HOST = os.getenv("DB_HOST", "localhost")

DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")  # Default user

DB_PASS = os.getenv("DB_PASS", "postgres")
DB_PORT = os.getenv("DB_PORT", "5433")


def init_db(start_date='2016-01-01'):
    df = pull_data_from_yfinance(
        start_date=start_date, end_date=formatted_date)
    filename = f"btc_data_{start_date}.parquet"
    save_to_parquet(df, filename=filename)

    try:
        logger.info("Connecting to the PostgreSQL database...")
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        cursor = connection.cursor()
        logger.info("Creating table and inserting data...")
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS market_data (
            trading_date DATE PRIMARY KEY,     -- The index (Date)
            open_price NUMERIC,                -- NUMERIC is best for money (exact precision)
            high_price NUMERIC,
            low_price NUMERIC,
            close_price NUMERIC,
            volume BIGINT                  -- BIGINT for large volumes (integers)
        );
        '''
        cursor.execute(create_table_query)
        df_clean = df.reset_index()
        data_list = [tuple(x) for x in df_clean.to_numpy()]
        insert_query = """
            INSERT INTO market_data (
                trading_date, 
                open_price, 
                high_price, 
                low_price, 
                close_price, 
                volume
            ) VALUES %s
            ON CONFLICT (trading_date) DO NOTHING;
        """
        extras.execute_values(
            cursor,
            insert_query,
            data_list
        )
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        logger.error(f"Error while interacting with the database: {e}")
        return None
    return True


if __name__ == "__main__":
    init_db()
