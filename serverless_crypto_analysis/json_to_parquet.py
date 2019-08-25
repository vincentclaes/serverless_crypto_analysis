from json2parquet import convert_json, ingest_data, write_parquet
import json
import pandas as pd
data_ = ingest_data([{ "name":"John", "age":30, "car":None }])
write_parquet(data_, 'data-as-parquet.parquet')

df = pd.read_parquet('data-as-parquet.parquet', engine='pyarrow')
df