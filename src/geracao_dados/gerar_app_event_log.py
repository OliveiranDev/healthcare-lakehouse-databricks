import os
import numpy as np
import pandas as pd
import uuid

SEED = 42
np.random.seed(SEED)

N_EVENTS = 900_000

DATA_INICIO = pd.Timestamp("2024-01-01")
DATA_FIM = pd.Timestamp("2025-12-31 23:59:59")

CANAIS = ["app_android", "app_ios", "web"]
P_CANAL = [0.55, 0.25, 0.20]

EVENTOS = [
    "login",
    "2via_boleto",
    "agendar_consulta",
    "status_reembolso",
    "abrir_chamado",
    "atualizar_cadastro",
    "consultar_rede",
    "carteirinha_digital"
]
P_EVENTO = [0.25, 0.12, 0.10, 0.10, 0.08, 0.05, 0.20, 0.10]

HTTP_OK = [200, 201, 204]
HTTP_ERR = [400, 401, 403, 404, 408, 429, 500, 502, 503]

def gerar_logs(df_benef):
    beneficiarios = df_benef["beneficiario_id"].drop_duplicates().values

    event_id = np.arange(1, N_EVENTS + 1)

    # Beneficiário (com 2% faltante)
    benef = np.random.choice(beneficiarios, size=N_EVENTS, replace=True).astype("float")
    idx_null = np.random.choice(np.arange(N_EVENTS), size=int(0.02 * N_EVENTS), replace=False)
    benef[idx_null] = np.nan

    # Timestamps uniformes no período (em segundos)
    start = int(DATA_INICIO.timestamp())
    end = int(DATA_FIM.timestamp())
    ts = np.random.randint(start, end, size=N_EVENTS)
    timestamp_evento = pd.to_datetime(ts, unit="s")

    canal = np.random.choice(CANAIS, size=N_EVENTS, p=P_CANAL)
    evento = np.random.choice(EVENTOS, size=N_EVENTS, p=P_EVENTO)

    # Sessão: UUID (gerar string curta)
    sessao_id = [uuid.uuid4().hex[:16] for _ in range(N_EVENTS)]

    # Latência (ms) - lognormal
    lat = np.random.lognormal(mean=np.log(320), sigma=0.55, size=N_EVENTS)
    latencia_ms = np.round(lat, 0).astype(int)

    # HTTP status: maioria OK
    http_status = np.where(
        np.random.rand(N_EVENTS) < 0.93,
        np.random.choice(HTTP_OK, size=N_EVENTS),
        np.random.choice(HTTP_ERR, size=N_EVENTS)
    )

    df = pd.DataFrame({
        "event_id": event_id,
        "beneficiario_id": benef,
        "timestamp_evento": timestamp_evento,
        "canal": canal,
        "evento": evento,
        "sessao_id": sessao_id,
        "latencia_ms": latencia_ms,
        "http_status": http_status
    })

    return df

def injetar_sujeira(df):
    df = df.copy()
    n = len(df)

    # 0.5% outlier de latência
    idx = np.random.choice(df.index, size=int(0.005 * n), replace=False)
    df.loc[idx, "latencia_ms"] = df.loc[idx, "latencia_ms"] * np.random.randint(30, 80, size=len(idx))

    # 0.3% status inválido
    idx2 = np.random.choice(df.index, size=int(0.003 * n), replace=False)
    df.loc[idx2, "http_status"] = 999

    # 1% duplicidade de event_id
    dup = df.sample(frac=0.01, random_state=SEED)
    df = pd.concat([df, dup], ignore_index=True)

    return df

def main():
    df_benef = pd.read_csv("data/raw/cadastro_beneficiarios.csv")
    df = gerar_logs(df_benef)
    df = injetar_sujeira(df)

    os.makedirs("data/raw", exist_ok=True)
    out_path = "data/raw/app_event_log.jsonl"

    df.to_json(out_path, orient="records", lines=True, date_format="iso", force_ascii=False)

    print(f"[OK] Gerado: {out_path} | linhas={len(df):,} | cols={df.shape[1]}")
    print("Beneficiario nulo:", int(df["beneficiario_id"].isna().sum()))

if __name__ == "__main__":
    main()