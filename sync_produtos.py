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


def to_float(valor, padrao=0.0) -> float:
    try:
        if valor is None or valor == "":
            return padrao
        return float(valor)
    except Exception:
        return padrao


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


def get_detalhe(endpoint: str, codigo, token: str):
    token_atual = token
    tentativas_429 = 0

    for tentativa in range(1, 6):
        url = f"https://api.egestor.com.br/api/v1/{endpoint}/{codigo}"
        headers = egestor_headers(token_atual)

        try:
            r = request_com_retry("GET", url, headers=headers, timeout=120, tentativas=4)
            log(f"detalhe {endpoint} {codigo} status: {r.status_code}")

            if r.status_code == 429:
                tentativas_429 += 1
                espera = min(10 * tentativas_429, 60)
                log(f"detalhe {endpoint} {codigo}: limite da API, aguardando {espera}s")
                time.sleep(espera)
                continue

            if r.status_code == 401:
                log(f"detalhe {endpoint} {codigo}: token expirado, renovando")
                token_atual = get_token()
                time.sleep(1)
                continue

            if r.status_code in [404, 410]:
                log(f"detalhe {endpoint} {codigo}: não encontrado ({r.status_code})")
                return None

            if r.status_code != 200:
                log(r.text[:1000])
                return None

            return r.json()

        except Exception as e:
            espera = min(5 * tentativa, 30)
            log(f"detalhe {endpoint} {codigo}: erro transitório, tentativa {tentativa}/5, aguardando {espera}s | erro: {e}")
            time.sleep(espera)

    log(f"detalhe {endpoint} {codigo}: falhou após várias tentativas, pulando")
    return None


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
# PRODUTOS
# =========================
def buscar_categoria_nome(cod_categoria, token: str):
    if cod_categoria is None or cod_categoria == "":
        return None

    resp = get_detalhe("categorias", cod_categoria, token)
    if not resp:
        return None

    return resp.get("nome") or resp.get("descricao")


def tratar_produtos(lista, token: str):
    resultado = []

    for item in lista:
        codigo = item.get("codigo") or item.get("id")
        if not codigo:
            continue

        detalhe = get_detalhe("produtos", codigo, token)
        if not detalhe:
            continue

        categoria_id = to_str(detalhe.get("codCategoria"))
        categoria_nome = buscar_categoria_nome(categoria_id, token) if categoria_id else None

        resultado.append({
            "id_origem": to_str(detalhe.get("codigo") or detalhe.get("id")),
            "codigo": to_str(detalhe.get("codigo") or ""),
            "nome": detalhe.get("descricao"),
            "categoria_id": categoria_id,
            "categoria_nome": categoria_nome if categoria_nome else "MERCADO",
            "unidade": detalhe.get("unidadeTributada") or detalhe.get("unidade"),
            "valor_venda": to_float(detalhe.get("precoVenda")),
            "custo": to_float(detalhe.get("precoCusto")),
            "estoque": to_float(detalhe.get("estoque")),
            "situacao": "OK",
        })

        time.sleep(0.05)

    return resultado


# =========================
# MAIN
# =========================
def main():
    log("🚀 INICIOU SYNC PRODUTOS")

    token = get_token()

    produtos = get_data_paginado("produtos", token)
    produtos_tratados = tratar_produtos(produtos, token)

    log(f"qtd produtos tratados: {len(produtos_tratados)}")

    enviar_supabase("eg_produtos", produtos_tratados)

    log("✅ FINALIZADO SYNC PRODUTOS")


if __name__ == "__main__":
    main()
