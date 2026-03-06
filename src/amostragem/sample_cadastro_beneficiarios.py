import pandas as pd

df = pd.read_csv("data/raw/cadastro_beneficiarios.csv")

sample = df.sample(200)

sample.to_csv(
    "data/sample/cadastro_beneficiarios_sample.csv",
    index=False
)