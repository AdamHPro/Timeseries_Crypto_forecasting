import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import numpy as np
import xgboost as xgb
import logging

logger = logging.getLogger(__name__)

# This must be put in the "main.py" file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

CURRENT_DIR = Path(__file__).resolve().parent.parent
folder_data_lake = CURRENT_DIR.parent / "data_lake" / "btc_usd"


def extract_df():
    logging.info("Extracting data from Parquet data lake...")
    table = pq.read_table(folder_data_lake)
    df = table.to_pandas()
    df = df.reset_index()
    df.columns = [col[0] for col in df.columns]
    df = df.drop_duplicates(subset=['Date'], keep='last')
    return df


def create_features_for_xgboost(df):
    logging.info("Creating features for XGBoost model...")
    # --- Step 0: Data Cleaning ---
    # Convert 'Date' to datetime objects and sort chronologically
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date')
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # Clean 'Volume' (e.g., "10K" -> 10000) if it is a string
    if df['Volume'].dtype == object:
        df['Volume'] = df['Volume'].replace(
            {'K': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)

    # --- Step 1: Feature Engineering ---

    # 1. Log Returns
    # We use log returns because they are time-additive and more stable than raw prices
    df['log_ret'] = np.log(df['Close'] / df['Close'].shift(1))

    # 2. Lag Features (Memory)
    # What happened yesterday? (Lag 1)
    df['ret_lag1'] = df['log_ret'].shift(1)
    # What happened one week ago? (Lag 7 - captures weekly cycles)
    df['ret_lag7'] = df['log_ret'].shift(7)
    # Volume from yesterday (Volume often precedes price movement)
    df['vol_lag1'] = df['Volume'].shift(1)

    # 3. Rolling Statistics (Trend & Volatility)
    # 30-day Moving Average (General monthly trend)
    df['ma_30'] = df['Close'].rolling(window=30).mean()

    # 30-day Standard Deviation (Is the market calm or chaotic?)
    df['std_30'] = df['log_ret'].rolling(window=30).std()

    # 4. Technical Indicators / Market Psychology
    # Distance from the Moving Average:
    # If positive/high -> Price is far above average (Potential bubble/overbought)
    # If negative/low -> Price is far below average (Potential oversold)
    df['dist_ma30'] = (df['Close'] / df['ma_30']) - 1

    # Daily Range: Normalized difference between High and Low
    # Shows intraday panic or euphoria
    df['daily_range'] = (df['High'] - df['Low']) / df['Close']
    # 5. Time Features
    # Crypto markets behave differently on weekends (Banks are closed)
    # 0 = Monday, 6 = Sunday
    df['day_of_week'] = df.index.dayofweek

    # --- Step 2: The Target ---
    # We want to predict the Price 7 days into the future.
    # On calcule la variation entre le prix dans 7 jours et le prix d'aujourd'hui
    df['target'] = np.log(df['Close'].shift(-7) / df['Close'])

    # Remove empty rows (NaNs) created by shifting and rolling
    df = df.dropna()

    return df


def convert_to_float(df):
    logging.info("Converting price columns to float...")
    for col in ['Close', 'High', 'Low', 'Open']:
        df[col] = df[col].astype(str).str.replace(',', '').str.replace('$', '')
        df[col] = df[col].astype(float)
    return df


def correct_data_types(df):
    logging.info("Correcting data types for price columns...")
    df['Low'] = df['Low'].astype(str).str.replace(',', '').str.replace('$', '')
    df['Low'] = df['Low'].astype(float)

    df['Open'] = df['Open'].astype(str).str.replace(
        ',', '').str.replace('$', '')
    df['Open'] = df['Open'].astype(float)
    return df


def train_xgboost_model(df, features_to_drop=['target', 'Open', 'High', 'Low']):
    logging.info("Training XGBoost model...")
    # We remove rows where 'target' is NaN (the last 7 days) because we can't learn from them.
    df_train = df.dropna(subset=['target'])
    X_train = df_train.drop(columns=features_to_drop)
    y_train = df_train['target']
    X_latest = df.drop(columns=features_to_drop).tail(1)

    model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=100)
    model.fit(X_train, y_train)
    # --- Step 3: Predict the Future ---
    # We use the latest available data (X_latest) to forecast
    prediction_log_ret = model.predict(X_latest)

    # Convert log return back to percentage for human readability
    predicted_return_pct = (np.exp(prediction_log_ret) - 1) * 100

    logging.info(
        f"Predicted return for the next 7 days: {predicted_return_pct[0]:.2f}%")
    return predicted_return_pct


def training_task(features_to_drop=['target', 'Open', 'High', 'Low']):
    df = extract_df()
    df = convert_to_float(df)
    df = correct_data_types(df)
    df = create_features_for_xgboost(df)
    predicted_return = train_xgboost_model(df, features_to_drop)
    return predicted_return


if __name__ == "__main__":
    predicted_return = training_task()
    print(f"Predicted return for the next 7 days: {predicted_return[0]:.2f}%")
