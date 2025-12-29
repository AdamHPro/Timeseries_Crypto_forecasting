import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import os

CURRENT_DIR = Path(__file__).resolve().parent
folder_data_lake = CURRENT_DIR.parent / "data_lake" / "btc_usd"
table = pq.read_table(folder_data_lake)
df = table.to_pandas()
df = df.reset_index()

df.columns = [col[0] for col in df.columns]
