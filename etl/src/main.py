import logging
import os
from pathlib import Path
from src.config import get_db_config
from src.init_db import init_db, verify_db
from src.update_db import update_db, get_latest_date_in_db, save_permanent_backup_parquet, save_prediction, create_prediction_table
from src.xgboost_training import training_task
from src.data_fetching import pull_data_from_yfinance
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(filename)s:%(lineno)d | %(funcName)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)

CURRENT_DIR = Path(__file__).resolve().parent.parent
DATA_LAKE_PATH = CURRENT_DIR.parent / "data_lake" / "btc_usd"
output_dir = os.getenv("DATA_LAKE_PATH", DATA_LAKE_PATH)


def pipeline(output_dir=output_dir):
    """
    Main ETL pipeline function.
    Args:
        output_dir (Path): Directory to store data files.
    Returns:
        predicted_return (float): The predicted return from the model.
    """
    logger.info("Starting ETL Pipeline...")
    if verify_db() is False:
        logger.info("Initializing database...")
        init_db(output_dir)
    logger.info("Updating database with new data...")
    db_config = get_db_config()
    start_date = get_latest_date_in_db(db_config)  # Get latest date from DB
    end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    # Pull new data and save to parquet
    data_path = pull_data_from_yfinance(output_dir, start_date, end_date)
    # Save permanent backup in parquet
    save_permanent_backup_parquet(data_path, start_date, end_date)
    # Update DB with new data (from parquet to postgres)
    update_db(db_config, data_path)
    logger.info("Training XGBoost model...")
    # Clean Data and Train XGBoost model and get prediction
    predicted_return = training_task(output_dir)
    logger.info("Pipeline completed successfully.")
    create_prediction_table(db_config)  # Ensure prediction table exists
    # Save the predicted return to DB
    save_prediction(db_config, predicted_return[0])
    return predicted_return


if __name__ == "__main__":
    pipeline()
