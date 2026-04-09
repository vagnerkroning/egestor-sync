import os
import time
import requests
from datetime import datetime
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
from supabase import create_client

# =========================
# CONFIG
# =========================
EGESTOR_PERSONAL_TOKEN = os.getenv("EGESTOR_PERSONAL_TOKEN", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

if not EGESTOR_PERSONAL_TOKEN:
    raise ValueError("EGESTOR_PERSONAL_TOKEN não definido.")
if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL não definido.")
if not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("SUPABASE_SERVICE_ROLE_KEY não definido.")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# =========================
# UTIL
# =========================
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def to_str(valor, padrao="") -> str:
    if valor is None:
        return padrao
    return str(valor)


# =========================
# HTTP COM RETRY
# =========================
def request_com_retry(method: str, url: str, headers=None, json=None, timeout=120, tentativas=6):
    ultimo_erro = None

    for tentativa in range(1, tentativas + 1):
        try:
            if method == "GET":
                r = requests.get(url, headers=headers, timeout=timeout)
            elif method == "POST":
                r = requests.post(url, headers=headers, json=json, timeout=timeout)
            else:
                raise ValueError(f"Método inválido: {method}")

            return r

        except (SSLError, ConnectionError, Timeout, RequestException) as e:
            ultimo_erro = e
            espera = min(3 * tentativa, 20)
            log(f"erro de conexão {method} {url} | tentativa {tentativa}/{tentativas} | aguardando {espera}s | erro: {e}")
            time.sleep(espera)

    raise ultimo_erro


# =========================
# AUTH EGESTOR
# =========================
def get_token() -> str:
    url = "https://api.egestor.com.br/api/oauth/access_token"
    payload = {
        "grant_type": "personal",
        "personal_token": EGESTOR_PERSONAL_TOKEN,
    }

    r = request_com_retry("POST", url, json=payload, timeout=60, tentativas=5)
    log(f"AUTH status: {r.status_code}")

    if r.status_code != 200:
        log(r.text[:500])
        r.raise_for_status()

    body = r.json()
    access_token = body.get("access_token")
    if not access_token:
        raise Exception("Não veio access_token do eGestor.")
    return access_token


def egestor_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# =========================
# API EGESTOR
# =========================
def get_data_paginado(endpoint: str, token: str):
    all_data = []
    page = 1
    tentativas_429 = 0
    token_atual = token

    while True:
        url = f"https://api.egestor.com.br/api/v1/{endpoint}?page={page}"
        headers = egestor_headers(token_atual)

        r = request_com_retry("GET", url, headers=headers, timeout=120, tentativas=5)
        log(f"{endpoint} página {page} status: {r.status_code}")

        if r.status_code == 429:
            tentativas_429 += 1
            espera = min(10 * tentativas_429, 60)
            log(f"{endpoint} página {page}: limite da API, aguardando {espera}s")
            time.sleep(espera)
            continue

        if r.status_code == 401:
            log(f"{endpoint} página {page}: token expirado, renovando")
            token_atual = get_token()
            time.sleep(1)
            continue

        tentativas_429 = 0

        if r.status_code != 200:
            log(r.text[:1000])
            break

        data = r.json().get("data", [])

        if not data:
            log(f"{endpoint} página {page}: vazia, encerrando")
            break

        all_data.extend(data)
        log(f"{endpoint} página {page}: {len(data)} registros")

        page += 1
        time.sleep(0.1)

    log(f"TOTAL {endpoint}: {len(all_data)} registros")
    return all_data


# =========================
# SUPABASE
# =========================
def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def enviar_supabase(tabela: str, dados) -> None:
    if not dados:
        log(f"{tabela}: nada para enviar")
        return

    url = f"{SUPABASE_URL}/rest/v1/{tabela}?on_conflict=id_origem"
    headers = {
        **supabase_headers(),
        "Prefer": "resolution=merge-duplicates,return=representation",
    }

    r = request_com_retry("POST", url, headers=headers, json=dados, timeout=120, tentativas=5)
    log(f"{tabela} supabase status: {r.status_code}")

    if r.status_code >= 300:
        log(r.text[:1000])

    r.raise_for_status()


# =========================
# TRATAMENTO PLANO DE CONTAS
# =========================
def tratar_plano_contas(lista):
    resultado = []

    for item in lista:
        registro = {
            "id_origem": item.get("codigo") or item.get("id"),
            "nome": item.get("nome") or item.get("descricao"),
            "grupo_dre": item.get("grupo_dre"),
            "subgrupo_dre": item.get("subgrupo_dre"),
        }
        resultado.append(registro)

    return resultado


# =========================
# MAIN
# =========================
def main():
    log("🚀 INICIOU SYNC PLANO DE CONTAS")

    token = get_token()

    # conforme a API do eGestor que você já vem usando
    plano_contas = get_data_paginado("planoContas", token)

    if not plano_contas:
        log("⚠️ nenhum plano de contas retornado")
        return

    plano_contas_tratados = tratar_plano_contas(plano_contas)
    log(f"qtd plano_contas tratados: {len(plano_contas_tratados)}")

    enviar_supabase("eg_plano_contas", plano_contas_tratados)

    log("✅ FINALIZADO SYNC PLANO DE CONTAS")


if __name__ == "__main__":
    main()
