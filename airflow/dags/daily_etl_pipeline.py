import pendulum
from airflow.decorators import dag, task

# Pipeline imports
import logging
import os
from pathlib import Path
from etl.src.config import get_db_config
from etl.src.init_db import init_db
from etl.src.update_db import update_db, get_latest_date_in_db, save_permanent_backup_parquet, save_prediction, create_prediction_table
from etl.src.xgboost_training import training_task
from etl.src.data_fetching import pull_data_from_yfinance
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)

# Default arguments for the DAG (retries, owner, etc.)
default_args = {
    'owner': 'me',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

# Define the DAG context
# schedule_interval: Here it runs daily at 6 AM. Use None for manual trigger only.
# catchup=False: Critical! Prevents Airflow from running all non-triggered past dates.


@dag(
    dag_id='mon_premier_etl_moderne',
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['etl', 'learning']
)
def etl_pipeline():

    # 1. Extract Task
    @task()
    def extract_data():
        # Simulate extraction logic
        raw_data = {"id": 1, "amount": 100, "currency": "USD"}
        print(f"Extracted: {raw_data}")
        return raw_data

    # 2. Transform Task
    # Airflow automatically handles data passing (XComs) between tasks
    @task()
    def transform_data(data: dict):
        data['amount_eur'] = data['amount'] * 0.92
        print(f"Transformed: {data}")
        return data

    # 3. Load Task
    @task()
    def load_data(data: dict):
        print(f"Loading data to database: {data}")
        # Insert logic here
        return "Success"

    # Define dependencies explicitly using function calls
    raw = extract_data()
    clean = transform_data(raw)
    load_data(clean)


# Instantiate the DAG
dag_instance = etl_pipeline()
