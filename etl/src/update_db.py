import logging
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta
from config import pull_db_config
from data_fetching import pull_data_from_yfinance, save_to_parquet

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


def update_db(start_date=None, end_date=None):
    if start_date == None:
        start_date = get_latest_date_in_db()
    if end_date == None:
        end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    df = pull_data_from_yfinance(start_date, end_date)
    filename = f"btc_data_{start_date}_{end_date}.parquet"

    output_dir = save_to_parquet(df, filename=filename)
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
        return output_dir

    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        return None
