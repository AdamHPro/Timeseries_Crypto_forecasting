import logging
from init_db import init_db
from update_db import update_db
from xgboost_training import training_task


logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def pipeline(init=False):
    logger.info("Starting ETL Pipeline...")
    if init:
        logger.info("Initializing database...")
        init_db()
    logger.info("Updating database with new data...")
    update_db()
    logger.info("Training XGBoost model...")
    predicted_return = training_task()
    logger.info("Pipeline completed successfully.")
    return predicted_return


if __name__ == "__main__":
    pipeline()
