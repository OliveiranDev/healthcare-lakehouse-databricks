import os
import numpy as np
import pandas as pd

SEED = 42
np.random.seed(SEED)

N_MANIFESTACOES = 80_000

DATA_INICIO = pd.Timestamp("2024-01-01")
DATA_FIM = pd.Timestamp("2025-12-31")

CANAIS = ["telefone", "app", "chat", "ouvidoria", "email"]
P_CANAL = [0.40, 0.25, 0.20, 0.10, 0.05]

CATEGORIAS = ["cobranca", "rede_credenciada", "autorizacao", "reembolso", "atendimento", "cancelamento"]
P_CATEG = [0.22, 0.18, 0.18, 0.16, 0.18, 0.08]

STATUS = ["resolvido", "pendente"]
P_STATUS = [0.92, 0.08]

def gerar_manifestacoes(df_benef: pd.DataFrame) -> pd.DataFrame:
    beneficiarios = df_benef["beneficiario_id"].drop_duplicates().values

    protocolo_id = np.arange(1, N_MANIFESTACOES + 1)

    benef_id = np.random.choice(beneficiarios, size=N_MANIFESTACOES, replace=True)
    datas = pd.to_datetime(
        np.random.choice(pd.date_range(DATA_INICIO, DATA_FIM, freq="D"), size=N_MANIFESTACOES)
    )
    canal = np.random.choice(CANAIS, size=N_MANIFESTACOES, p=P_CANAL)
    categoria = np.random.choice(CATEGORIAS, size=N_MANIFESTACOES, p=P_CATEG)
    status = np.random.choice(STATUS, size=N_MANIFESTACOES, p=P_STATUS)

    # SLA e tempo de resolução (em horas)
    sla_horas = np.random.choice([24, 48, 72, 96, 120], size=N_MANIFESTACOES, p=[0.25, 0.30, 0.25, 0.15, 0.05])

    # tempo_resolucao: resolvido tende a ser menor, pendente maior
    tempo_resolucao = np.where(
        status == "resolvido",
        np.random.gamma(shape=2.0, scale=18.0, size=N_MANIFESTACOES),  # média ~36h
        np.random.gamma(shape=2.5, scale=30.0, size=N_MANIFESTACOES)   # média ~75h
    )
    tempo_resolucao = np.round(tempo_resolucao, 1)

    # CSAT (1-5) e NPS (0-10)
    csat = np.random.choice([1,2,3,4,5], size=N_MANIFESTACOES, p=[0.08,0.12,0.22,0.32,0.26])
    nps = np.random.choice(list(range(0,11)), size=N_MANIFESTACOES,
                           p=[0.02,0.02,0.03,0.04,0.06,0.08,0.12,0.18,0.20,0.15,0.10])

    df = pd.DataFrame({
        "protocolo_id": protocolo_id,
        "beneficiario_id": benef_id,
        "data_abertura": datas,
        "canal": canal,
        "categoria": categoria,
        "status": status,
        "sla_horas": sla_horas,
        "tempo_resolucao_horas": tempo_resolucao,
        "csat": csat,
        "nps": nps,
    })

    return df

def injetar_sujeira(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    n = len(df)

    # 3% nps nulo
    idx = np.random.choice(df.index, size=int(0.03 * n), replace=False)
    df.loc[idx, "nps"] = None

    # 2% categoria com variação de caixa/acentos
    idx2 = np.random.choice(df.index, size=int(0.02 * n), replace=False)
    df.loc[idx2, "categoria"] = df.loc[idx2, "categoria"].replace({
        "cobranca": "Cobrança",
        "rede_credenciada": "REDE_CREDENCIADA",
        "autorizacao": "Autorização"
    })

    # 0.5% tempo_resolucao negativo (erro)
    idx3 = np.random.choice(df.index, size=int(0.005 * n), replace=False)
    df.loc[idx3, "tempo_resolucao_horas"] = -1 * df.loc[idx3, "tempo_resolucao_horas"].abs()

    # 0.5% duplicados de protocolo (duplica linhas)
    dup = df.sample(frac=0.005, random_state=SEED)
    df = pd.concat([df, dup], ignore_index=True)

    return df

def main():
    df_benef = pd.read_csv("data/raw/cadastro_beneficiarios.csv")

    df = gerar_manifestacoes(df_benef)
    df = injetar_sujeira(df)

    os.makedirs("data/raw", exist_ok=True)
    out_path = "data/raw/sac_srp_manifestacoes.xlsx"

    df.to_excel(out_path, index=False)

    print(f"[OK] Gerado: {out_path} | linhas={len(df):,} | cols={df.shape[1]}")
    print("Nulos NPS:", int(df["nps"].isna().sum()))

if __name__ == "__main__":
    main()