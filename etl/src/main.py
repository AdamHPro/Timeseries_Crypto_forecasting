import logging
from pathlib import Path
from init_db import init_db
from update_db import update_db, get_latest_date_in_db, save_permanent_backup_parquet
from xgboost_training import training_task
from data_fetching import pull_data_from_yfinance
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)

CURRENT_DIR = Path(__file__).resolve().parent.parent
output_dir = CURRENT_DIR.parent / "data_lake" / "btc_usd"


def pipeline(init=False, output_dir=output_dir):
    logger.info("Starting ETL Pipeline...")
    if init:
        logger.info("Initializing database...")
        init_db()
    logger.info("Updating database with new data...")
    start_date = get_latest_date_in_db()  # Get latest date from DB
    end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    # Pull new data and save to parquet
    data_path = pull_data_from_yfinance(output_dir, start_date, end_date)
    # Save permanent backup in parquet
    save_permanent_backup_parquet(data_path, start_date, end_date)
    update_db(data_path)  # Update DB with new data (from parquet to postgres)
    logger.info("Training XGBoost model...")
    # Clean Data and Train XGBoost model and get prediction
    predicted_return = training_task(output_dir)
    logger.info("Pipeline completed successfully.")
    return predicted_return


if __name__ == "__main__":
    pipeline()
