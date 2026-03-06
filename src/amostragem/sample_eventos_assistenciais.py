import pandas as pd

df = pd.read_parquet("data/raw/eventos_assistenciais.parquet")

df.sample(500).to_parquet(
    "data/sample/eventos_assistenciais_sample.parquet"
)