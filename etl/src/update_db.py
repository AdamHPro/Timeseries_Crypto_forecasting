import logging
import shutil
import os
import pandas as pd
import psycopg2
from psycopg2 import extras
from config import pull_db_config

logger = logging.getLogger(__name__)
db_config = pull_db_config()


def get_latest_date_in_db():
    try:
        logger.info(
            "Connecting to the PostgreSQL database to get the latest date...")
        connection = psycopg2.connect(
            user=db_config["user"],
            password=db_config["pass"],
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["name"]
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


def save_permanent_backup_parquet(data_path, start_date, end_date):
    filename = f"btc_data_{start_date}_{end_date}.parquet"
    logger.debug(
        f"Saving permanent backup parquet file from {data_path} as {filename}")
    shutil.copy(data_path, os.path.join(os.path.dirname(data_path), filename))


def update_db(data_path):
    df = pd.read_parquet(data_path)

    try:
        connection = psycopg2.connect(
            user=db_config["user"],
            password=db_config["pass"],
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["name"]
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
