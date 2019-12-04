import pandas as pd
import sqlite3

conn = sqlite3.connect(
    "/Users/vincent/Workspace/data/crypto_analysis/coinmarketcap_data.db"
)
df = pd.read_sql_query("SELECT * FROM crypto_data", conn)
df.to_parquet(
    "/Users/vincent/Workspace/data/crypto_analysis/coinmarketcap_data.parquet",
    compression="gzip",
)
