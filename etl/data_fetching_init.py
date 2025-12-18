import investpy
from datetime import datetime
import logging
import psycopg2
from psycopg2 import extras

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

now = datetime.now()
formatted_date = now.strftime("%d/%m/%Y")

# Configuration for database connection
DB_HOST = "localhost"  # Usually localhost if Docker maps the port
DB_NAME = "postgres"  # Default DB name, change if you created another
DB_USER = "postgres"  # Default user
DB_PASS = "postgres"  # The one defined in POSTGRES_PASSWORD
DB_PORT = "5433"


def init_db():
    try:
        logger.info("Fetching historical crypto data from investpy...")
        df = investpy.get_crypto_historical_data(
            crypto='bitcoin', from_date='01/01/2016', to_date=formatted_date)
    except Exception as e:
        logger.error(f"Error while fetching data from investpy: {e}")
        return None

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
            volume BIGINT,                     -- BIGINT for large volumes (integers)
            currency VARCHAR(10)               -- Short text for 'USD', 'EUR', etc.
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
                volume, 
                currency
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


init_db()
