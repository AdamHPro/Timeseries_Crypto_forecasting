from datetime import datetime
import logging
import psycopg2
from psycopg2 import extras
import pandas as pd
import os
import shutil
from pathlib import Path
from src.config import get_db_config
from src.data_fetching import pull_data_from_yfinance


logger = logging.getLogger(__name__)


now = datetime.now()
formatted_date = now.strftime("%Y-%m-%d")
db_config = get_db_config()


def verify_db(db_config=db_config, table_name="market_data"):
    """
    Verifies the connection to the PostgreSQL database.
    """
    try:
        connection = psycopg2.connect(
            user=db_config["user"],
            password=db_config["pass"],
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["name"]
        )
        cursor = connection.cursor()
        cursor.execute(f"SELECT to_regclass('{table_name}');")
        result = cursor.fetchone()
        if result[0] is None:
            logger.warning(
                f"Table '{table_name}' does not exist in the database.")
            return False
        else:
            logger.info(f"Table '{table_name}' exists in the database.")
            return True
    except Exception as e:
        logger.error(f"Error while verifying table existence: {e}")
        return False


def init_db(output_dir, db_config=db_config, start_date='2016-01-01'):
    """
    Initializes the PostgreSQL database with historical BTC-USD data from yfinance.
    """
    initial_data_path = pull_data_from_yfinance(output_dir,
                                                start_date=start_date, end_date=formatted_date)
    logger.info("Initial data pulled from yfinance")
    df = pd.read_parquet(initial_data_path)
    logger.info("Initial db data fetched from parquet")
    filename = f"btc_data_{start_date}.parquet"
    logger.info("Saving permanent backup of initial data...")
    shutil.copy(initial_data_path, os.path.join(
        Path(initial_data_path).parent, filename))

    try:
        logger.info("Connecting to the PostgreSQL database...")
        connection = psycopg2.connect(
            user=db_config["user"],
            password=db_config["pass"],
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["name"]
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
