import yfinance as yf
import os
import logging

logger = logging.getLogger(__name__)


def save_to_parquet(df, filename="btc_data.parquet"):
    """
    Saves the dataframe to a parquet file acting as a Data Lake / Backup.
    """
    try:
        # Create directory if it doesn't exist
        output_dir = "data_lake/btc_usd"
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, filename)

        # Save to parquet (with compression usually enabled by default)
        df.to_parquet(path)
        logger.info(f"Data successfully saved to Parquet at {path}")
    except Exception as e:
        logger.error(f"Failed to save Parquet file: {e}")


def pull_data_from_yfinance(start_date, end_date):
    try:
        logger.info(
            f"Fetching historical crypto data from yfinance from {start_date} to {end_date}...")
        df = yf.download("BTC-USD", start=start_date,
                         end=end_date, progress=False)
        return df
    except Exception as e:
        logger.error(f"Error while fetching data from yfinance: {e}")
        return None
