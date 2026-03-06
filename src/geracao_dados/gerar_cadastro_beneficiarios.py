import os
import hashlib
import numpy as np
import pandas as pd
from datetime import date
from faker import Faker

SEED = 42
np.random.seed(SEED)
fake = Faker("pt_BR")
Faker.seed(SEED)

N_BENEF = 25_000
DATA_INICIO = pd.Timestamp("2024-01-01")
DATA_FIM = pd.Timestamp("2025-12-31")

UFS = ["SP", "RJ", "MG", "PR", "RS", "SC", "BA", "PE", "CE", "DF", "GO"]
CANAIS = ["corretor", "digital", "empregador", "adesao"]
SEGMENTOS = ["individual_familiar", "empresarial", "coletivo_adesao"]

def cpf_hash() -> str:
    # CPF fake + hash (não PII real)
    cpf = fake.cpf()
    return hashlib.sha256(cpf.encode("utf-8")).hexdigest()

def gerar_beneficiarios(n: int) -> pd.DataFrame:
    ids = np.arange(1, n + 1)

    # idades realistas (concentração 25-55)
    idades = np.clip(np.random.normal(loc=42, scale=14, size=n).round().astype(int), 0, 90)
    hoje = pd.Timestamp("2025-12-31")
    datas_nasc = hoje - pd.to_timedelta(idades * 365.25, unit="D")

    sexo = np.random.choice(["F", "M"], size=n, p=[0.52, 0.48]).astype(object)
    uf = np.random.choice(UFS, size=n, p=[0.35, 0.12, 0.10, 0.10, 0.08, 0.07, 0.06, 0.04, 0.03, 0.03, 0.02]).astype(object)
    canal = np.random.choice(CANAIS, size=n, p=[0.45, 0.25, 0.20, 0.10]).astype(object)

    # segmento de vínculo (tipo plano principal)
    segmento = np.random.choice(SEGMENTOS, size=n, p=[0.45, 0.40, 0.15]).astype(object)

    # município sintético (não precisa ser perfeito; importante é consistência)
    municipios = np.array([fake.city() for _ in range(n)], dtype=object)

    df = pd.DataFrame({
        "beneficiario_id": ids,
        "cpf_hash": [cpf_hash() for _ in range(n)],
        "data_nascimento": datas_nasc.date,
        "idade": idades,
        "sexo": sexo,
        "uf": uf,
        "municipio": municipios,
        "canal_aquisicao": canal,
        "segmento_vinculo": segmento,
    })

    return df

def injetar_sujeira(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)

    # Nulos controlados
    for col, pct in [("sexo", 0.02), ("municipio", 0.03)]:
        idx = np.random.choice(df.index, size=int(pct * n), replace=False)
        df.loc[idx, col] = None

    # Padronização quebrada (caixa/acentos)
    idx = np.random.choice(df.index, size=int(0.05 * n), replace=False)
    df.loc[idx, "municipio"] = df.loc[idx, "municipio"].astype(str).str.upper()

    # Duplicidade (0,5%)
    dup = df.sample(frac=0.005, random_state=SEED)
    df = pd.concat([df, dup], ignore_index=True)

    return df

def main():
    df = gerar_beneficiarios(N_BENEF)
    df = injetar_sujeira(df)

    out_dir = os.path.join("data", "raw")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, "cadastro_beneficiarios.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")

    print(f"[OK] Gerado: {out_path} | linhas={len(df):,} | cols={df.shape[1]}")

if __name__ == "__main__":
    main()