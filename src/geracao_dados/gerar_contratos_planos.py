import os
import numpy as np
import pandas as pd

SEED = 42
np.random.seed(SEED)

DATA_INICIO_PERIODO = pd.Timestamp("2024-01-01")
DATA_FIM_PERIODO = pd.Timestamp("2025-12-31")

CHURN_ANUAL = 0.27  # Aproximado setor
TIPOS_PLANO = ["individual_familiar", "empresarial", "coletivo_adesao"]

def gerar_contratos(df_benef):
    n = len(df_benef)

    contrato_id = np.arange(1, n + 1)

    # Datas de início aleatórias dentro do período
    datas_inicio = pd.to_datetime(
        np.random.choice(
            pd.date_range(DATA_INICIO_PERIODO, DATA_FIM_PERIODO, freq="D"),
            size=n
        )
    )

    # Valor mensal baseado no tipo
    valores = []
    churn_flags = []
    datas_fim = []

    for i in range(n):
        tipo = df_benef.loc[i, "segmento_vinculo"]

        if tipo == "individual_familiar":
            valor = np.random.normal(750, 120)
            churn_prob = CHURN_ANUAL + 0.03
        elif tipo == "empresarial":
            valor = np.random.normal(620, 90)
            churn_prob = CHURN_ANUAL - 0.05
        else:
            valor = np.random.normal(680, 110)
            churn_prob = CHURN_ANUAL

        valor = max(300, round(valor, 2))
        valores.append(valor)

        churn = np.random.rand() < churn_prob
        churn_flags.append(churn)

        if churn:
            fim = datas_inicio[i] + pd.to_timedelta(
                np.random.randint(30, 540), unit="D"
            )
            if fim > DATA_FIM_PERIODO:
                fim = None
        else:
            fim = None

        datas_fim.append(fim)

    df = pd.DataFrame({
        "contrato_id": contrato_id,
        "beneficiario_id": df_benef["beneficiario_id"],
        "tipo_plano": df_benef["segmento_vinculo"],
        "data_inicio_vigencia": datas_inicio,
        "data_fim_vigencia": datas_fim,
        "valor_mensal": valores,
        "coparticipacao_flag": np.random.choice([True, False], size=n, p=[0.6, 0.4])
    })

    return df

def injetar_sujeira(df):
    n = len(df)

    # 1% inconsistência temporal
    idx = np.random.choice(df.index, size=int(0.01 * n), replace=False)
    df.loc[idx, "data_fim_vigencia"] = df.loc[idx, "data_inicio_vigencia"] - pd.to_timedelta(5, unit="D")

    # 0.5% valor mensal nulo
    idx2 = np.random.choice(df.index, size=int(0.005 * n), replace=False)
    df.loc[idx2, "valor_mensal"] = None

    return df

def main():
    df_benef = pd.read_csv("data/raw/cadastro_beneficiarios.csv")
    df_benef = df_benef.drop_duplicates(subset=["beneficiario_id"]).reset_index(drop=True)

    df_contratos = gerar_contratos(df_benef)
    df_contratos = injetar_sujeira(df_contratos)

    os.makedirs("data/raw", exist_ok=True)
    df_contratos.to_csv("data/raw/contratos_planos.csv", index=False)

    churn_real = df_contratos["data_fim_vigencia"].notnull().mean()
    print(f"[OK] Gerado contratos_planos.csv | linhas={len(df_contratos):,}")
    print(f"Churn observado no período: {round(churn_real*100,2)}%")

if __name__ == "__main__":
    main()