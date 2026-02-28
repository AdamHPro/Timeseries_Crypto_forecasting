import logging
import shutil
import os
import pandas as pd
import psycopg2
from psycopg2 import extras
from src.config import get_db_config
from contextlib import contextmanager

logger = logging.getLogger(__name__)
db_config = get_db_config()


@contextmanager
def get_db_connection(db_config=db_config):
    """
    Context manager for PostgreSQL database connection.
    """
    connection = None
    try:
        connection = psycopg2.connect(
            user=db_config["user"],
            password=db_config["pass"],
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["name"]
        )
        yield connection
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()  # Rollback transaction on error
        logger.error(f"Database transaction failed: {e}")
        raise e
    finally:
        if connection:
            connection.close()  # Always close the connection


def get_latest_date_in_db(db_config=db_config):
    """
    Fetches the latest trading date from the market_data table in the postgres database.
    """
    try:
        logger.info(
            "Connecting to the PostgreSQL database to get the latest date...")
        with get_db_connection(db_config) as connection:
            with connection.cursor() as cursor:
                query = "SELECT MAX(trading_date) FROM market_data;"
                cursor.execute(query)
                result = cursor.fetchone()
                latest_date = result[0] if result[0] is not None else '01/01/2016'
                logger.info(f"Latest date in DB: {latest_date}")
                return latest_date.strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"Error while fetching latest date from DB: {e}")
        return '2016-01-01'


def save_permanent_backup_parquet(data_path, start_date, end_date):
    """
    Saves a permanent backup of the parquet file with a timestamped filename.
    """
    filename = f"btc_data_{start_date}_{end_date}.parquet"
    logger.debug(
        f"Saving permanent backup parquet file from {data_path} as {filename}")
    shutil.copy(data_path, os.path.join(os.path.dirname(data_path), filename))


def update_db(db_config=db_config, data_path=None):
    """
    Updates the PostgreSQL database with new data from the parquet file.
    """
    df = pd.read_parquet(data_path)

    try:
        with get_db_connection(db_config) as connection:
            with connection.cursor() as cursor:
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
                logger.info(
                    f"Database update complete. Added {len(data_list)} records.")

    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        return None


def create_prediction_table(db_config=db_config):
    """
    Creates the predictions table if it does not exist.
    """
    query = '''
    CREATE TABLE IF NOT EXISTS predictions (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        model_version VARCHAR(50),
        predicted_return_pct NUMERIC
    );
    '''
    with get_db_connection(db_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            logger.info("Table 'predictions' checked/created.")


def save_prediction(db_config=db_config, value=None):
    """
    Saves the predicted return value into the predictions table.
    """
    query = "INSERT INTO predictions (predicted_return_pct, model_version) VALUES (%s, %s);"
    try:
        with get_db_connection(db_config) as conn:
            with conn.cursor() as cursor:
                # On marque 'v1' pour l'instant, utile pour tes futures améliorations
                cursor.execute(query, (float(value), 'xgboost_v1'))
                logger.info(f"Prediction saved: {value}%")
    except Exception as e:
        logger.error(f"Failed to save prediction: {e}")
