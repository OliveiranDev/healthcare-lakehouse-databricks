import os
import numpy as np
import pandas as pd

SEED = 42
np.random.seed(SEED)

PERIODO = pd.period_range("2024-01", "2025-12", freq="M")

STATUS = ["pago", "atraso", "renegociado", "cancelado"]
PESOS_STATUS = [0.88, 0.08, 0.03, 0.01]

def preparar_contratos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["data_inicio_vigencia"] = pd.to_datetime(df["data_inicio_vigencia"], errors="coerce")
    df["data_fim_vigencia"] = pd.to_datetime(df["data_fim_vigencia"], errors="coerce")
    df["valor_mensal"] = pd.to_numeric(df["valor_mensal"], errors="coerce")  # sem imputar
    return df

def gerar_faturas(df_contratos: pd.DataFrame) -> pd.DataFrame:
    registros = []

    for row in df_contratos.itertuples(index=False):
        benef_id = int(row.beneficiario_id)
        valor = row.valor_mensal  # pode ser NaN
        fim = row.data_fim_vigencia  # pode ser NaT

        for comp in PERIODO:
            comp_ts = comp.to_timestamp(how="start")

            # Se contrato tem fim e a competência é após o fim, não gera fatura
            if pd.notna(fim) and comp_ts > fim:
                continue

            status = np.random.choice(STATUS, p=PESOS_STATUS)

            # Se valor_mensal está nulo, preserva nulo (para praticar Silver)
            if pd.isna(valor):
                valor_faturado = np.nan
                valor_pago = np.nan
                data_pagamento = None
            else:
                valor_faturado = float(valor)

                if status == "pago":
                    valor_pago = float(valor)
                elif status == "cancelado":
                    valor_pago = 0.0
                else:
                    valor_pago = float(valor) * float(np.random.uniform(0.5, 1.0))

                data_pagamento = None
                if status in ["pago", "renegociado"]:
                    atraso_dias = int(np.random.randint(0, 15))
                    data_pagamento = (comp_ts + pd.to_timedelta(atraso_dias, unit="D")).date()

            registros.append({
                "beneficiario_id": benef_id,
                "competencia": str(comp),
                "valor_faturado": None if pd.isna(valor_faturado) else round(valor_faturado, 2),
                "valor_pago": None if pd.isna(valor_pago) else round(valor_pago, 2),
                "status_pagamento": status,
                "data_pagamento": data_pagamento
            })

    return pd.DataFrame(registros)

def injetar_sujeira(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 0.5% valor_pago maior que faturado (apenas onde faturado não é nulo)
    candidatos = df.index[df["valor_faturado"].notna()]
    idx = np.random.choice(candidatos, size=int(0.005 * len(df)), replace=False)
    df.loc[idx, "valor_pago"] = (df.loc[idx, "valor_faturado"].astype(float) * 1.2).round(2)

    # 0.3% data_pagamento faltante mesmo pago/renegociado (apenas onde faria sentido)
    candidatos2 = df.index[df["status_pagamento"].isin(["pago", "renegociado"]) & df["valor_faturado"].notna()]
    idx2 = np.random.choice(candidatos2, size=int(0.003 * len(df)), replace=False)
    df.loc[idx2, "data_pagamento"] = None

    return df

def main():
    df_contratos = pd.read_csv("data/raw/contratos_planos.csv")
    df_contratos = preparar_contratos(df_contratos)

    df_faturas = gerar_faturas(df_contratos)
    df_faturas = injetar_sujeira(df_faturas)

    os.makedirs("data/raw", exist_ok=True)
    out_path = "data/raw/faturas_pagamentos.jsonl"

    df_faturas.to_json(out_path, orient="records", lines=True, force_ascii=False)

    print(f"[OK] Gerado: {out_path} | linhas={len(df_faturas):,}")
    print("Distribuição status:")
    print(df_faturas["status_pagamento"].value_counts(normalize=True).round(3))
    print("Nulos em valor_faturado:", int(df_faturas["valor_faturado"].isna().sum()))
    print("Nulos em valor_pago:", int(df_faturas["valor_pago"].isna().sum()))

if __name__ == "__main__":
    main()