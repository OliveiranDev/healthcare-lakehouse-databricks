import pandas as pd
from pathlib import Path

IN_PATH = Path("data/raw/eventos_assistenciais.parquet")
OUT_PATH = Path("data/raw/eventos_assistenciais_compat.parquet")

# lê com pandas/pyarrow
df = pd.read_parquet(IN_PATH)

# garante datetime64[ns] (pandas)
# e remove qualquer timezone (se houver)
df["data_evento"] = pd.to_datetime(df["data_evento"], errors="coerce").dt.tz_localize(None)

# escreve com coerção para micros (compatível com Spark em geral)
df.to_parquet(OUT_PATH, index=False, engine="pyarrow", coerce_timestamps="us", allow_truncated_timestamps=True)

print(f"[OK] Gerado: {OUT_PATH} | linhas={len(df):,}")