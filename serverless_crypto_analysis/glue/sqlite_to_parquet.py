import pandas as pd
import sqlite3
import pyarrow as pa
conn = sqlite3.connect(
    "/Users/vincent/Desktop/coinmarketcap_data.db"
)
df = pd.read_sql_query("SELECT * FROM crypto_data", conn)

def convert_types(df):
    print(df.columns)
    types = [
        pd.to_numeric,
        pd.to_numeric,
        str,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        str,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        str,
        pd.to_numeric,
        pd.to_datetime,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
    ]
    df_updated = pd.DataFrame()
    column_type_mapping = zip(types, df.columns)
    for type_func, column in column_type_mapping:
        if not isinstance(type_func, type):
            print(column)
            df_updated[column] = type_func(df[column])
        else:
            df_updated[column] = df[column]
    return df_updated

df = pd.DataFrame(df.to_dict())
df = convert_types(df)
df.to_parquet(
    "/Users/vincent/Desktop/coinmarketcap_data.db.parquet",
    compression="gzip",
)
