import pandas as pd
from pathlib import Path

# Caminhos locais do seu projeto
INPUT_PATH = Path("data/raw/sac_srp_manifestacoes.xlsx")
OUTPUT_PATH = Path("data/raw/sac_srp_manifestacoes.csv")

def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {INPUT_PATH}")

    # Leitura do Excel
    df = pd.read_excel(INPUT_PATH, engine="openpyxl")

    # Salvar como CSV
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

    print(f"[OK] Conversão concluída:")
    print(f"Arquivo origem : {INPUT_PATH}")
    print(f"Arquivo destino: {OUTPUT_PATH}")
    print(f"Linhas: {len(df):,} | Colunas: {df.shape[1]}")

if __name__ == "__main__":
    main()