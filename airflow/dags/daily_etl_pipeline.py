import pendulum
from airflow.decorators import dag, task

# Pipeline imports
import logging
import os
from pathlib import Path
from src.config import get_db_config
from src.init_db import init_db
from src.update_db import update_db, get_latest_date_in_db, save_permanent_backup_parquet, save_prediction, create_prediction_table
from src.xgboost_training import training_task
from src.data_fetching import pull_data_from_yfinance
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)

CURRENT_DIR = Path(__file__).resolve().parent.parent
DATA_LAKE_PATH = CURRENT_DIR.parent / "data_lake" / "btc_usd"
output_dir = os.getenv("DATA_LAKE_PATH", DATA_LAKE_PATH)


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
    tags=['etl', 'pipeline']
)
def finance_ml_pipeline():

    @task(multiple_outputs=True)
    def extract_data(output_dir: str) -> str:
        # Get database configuration
        db_config = get_db_config()
        
        # Calculate dates
        start_date = get_latest_date_in_db(db_config)
        end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info(f"Extracting data from {start_date} to {end_date}")
        
        # Pull new data and save to parquet
        data_path = pull_data_from_yfinance(output_dir, start_date, end_date)
        
        # Return the path so downstream tasks can use it via XCom
        return {
                'data_path': data_path,
                'start_date': start_date,
                'end_date': end_date
            }

    @task
    def backup_to_parquet(data_path: str, start_date: str, end_date: str):
        # Save permanent backup in parquet
        logger.info(f"Backing up data from {data_path} for dates {start_date} to {end_date}")
        # Assuming your function can extract dates from the data or path
        save_permanent_backup_parquet(data_path, start_date, end_date)

    @task
    def load_to_db(data_path: str):
        # Update DB with new data (from parquet to postgres)
        db_config = get_db_config()
        logger.info("Loading new data to database...")
        update_db(db_config, data_path)

    @task
    def train_xgboost(output_dir: str) -> float:
        # Clean Data and Train XGBoost model and get prediction
        logger.info("Training XGBoost model...")
        predicted_return = training_task(output_dir)
        
        # Return only the float value for the next task
        return predicted_return[0]

    @task
    def save_model_prediction(prediction: float):
        # Ensure prediction table exists and save the predicted return
        db_config = get_db_config()
        create_prediction_table(db_config)
        save_prediction(db_config, prediction)
        logger.info(f"Successfully saved prediction: {prediction}")

    # --- DAG Execution Flow ---
    
    # Define output directory variable (could also be an Airflow Variable)
    output_directory = "/path/to/your/output"

    # 1. Extract data
    extracted_data = extract_data(output_dir)

    # 2. Parallel tasks: Backup and Database Load
    # Both tasks take the output from extract_data (file_path)
    backup_task = backup_to_parquet(
        data_path=extracted_data['data_path'],
        start_date=extracted_data['start_date'],
        end_date=extracted_data['end_date']
    )    
    load_task = load_to_db(data_path=extracted_data['data_path'])

    # 3. Train the model
    # We pass the output_dir, but we must ensure it runs AFTER the database is loaded
    model_prediction = train_xgboost(output_dir=extracted_data['data_path'])

    # 4. Save the prediction
    final_save = save_model_prediction(model_prediction)

    # Define explicit dependencies for tasks that don't pass XCom data directly
    # We want training to wait until the database load is complete
    load_task >> model_prediction >> final_save


# Instantiate the DAG
dag_instance = finance_ml_pipeline()
