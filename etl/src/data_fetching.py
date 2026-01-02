import yfinance as yf
import os
import logging

logger = logging.getLogger(__name__)


def save_to_parquet(df, output_dir, filename="btc_data.parquet"):
    """
    Saves the dataframe to a parquet file acting as a Data Lake / Backup.
    """
    try:
        # Create directory if it doesn't exist

        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, filename)

        # Save to parquet (with compression usually enabled by default)
        df.to_parquet(path)
        logger.info(f"Data successfully saved to Parquet at {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to save Parquet file: {e}")
        return None


def pull_data_from_yfinance(output_dir, start_date, end_date):
    try:
        logger.info(
            f"Fetching historical crypto data from yfinance from {start_date} to {end_date}...")
        df = yf.download("BTC-USD", start=start_date,
                         end=end_date, progress=False)
        return save_to_parquet(df, output_dir, filename="extraction_to_ingestion.parquet")
    except Exception as e:
        logger.error(f"Error while fetching data from yfinance: {e}")
        return None
