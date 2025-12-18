import investpy
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

now = datetime.now()
formatted_date = now.strftime("%d/%m/%Y")


def init_db():
    try:
        df = investpy.get_crypto_historical_data(
            crypto='bitcoin', from_date='01/01/2016', to_date=formatted_date)
        return df
    except Exception as e:
        logger.error(f"Error while fetching data from investpy: {e}")
        return None
