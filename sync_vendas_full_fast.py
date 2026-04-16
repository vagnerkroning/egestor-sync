import os
import time
import math
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

LOTE_UPSERT = int(os.getenv("LOTE_UPSERT", "500"))
LOTE_DELETE = int(os.getenv("LOTE_DELETE", "500"))
LOTE_SELECT = int(os.getenv("LOTE_SELECT", "1000"))

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
        time.sleep(0.05)

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
                log(f"ℹ️ {endpoint}/{codigo} não existe mais ({r.status_code}) - pulando")
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
# SUPABASE HELPERS
# =========================
def executar_supabase_com_retry(func, descricao="", tentativas=5):
    for tentativa in range(1, tentativas + 1):
        try:
            return func()
        except Exception as e:
            espera = min(tentativa * 3, 15)
            log(f"⚠️ erro Supabase em {descricao} | tentativa {tentativa}/{tentativas} | {str(e)}")
            if tentativa == tentativas:
                raise
            time.sleep(espera)


def upsert_lote(tabela: str, registros: list[dict], on_conflict: str = "id_origem"):
    if not registros:
        return

    for i in range(0, len(registros), LOTE_UPSERT):
        lote = registros[i:i + LOTE_UPSERT]
        executar_supabase_com_retry(
            lambda lote=lote: supabase.table(tabela).upsert(lote, on_conflict=on_conflict).execute(),
            descricao=f"upsert {tabela} lote {i}-{i + len(lote)}"
        )
        log(f"✅ upsert {tabela} lote {i}-{i + len(lote)}")


def carregar_mapa_produtos():
    log("📦 carregando mapa de produtos do Supabase")
    mapa = {}
    inicio = 0

    while True:
        fim = inicio + LOTE_SELECT - 1
        resp = executar_supabase_com_retry(
            lambda inicio=inicio, fim=fim: supabase.table("eg_produtos")
            .select("id_origem,categoria_id,categoria_nome,nome")
            .range(inicio, fim)
            .execute(),
            descricao=f"select eg_produtos {inicio}-{fim}"
        )

        rows = resp.data or []
        if not rows:
            break

        for row in rows:
            produto_id = to_str(row.get("id_origem"))
            mapa[produto_id] = {
                "categoria_id": to_str(row.get("categoria_id")),
                "categoria_nome": row.get("categoria_nome") or "MERCADO",
                "nome": row.get("nome") or "",
            }

        inicio += LOTE_SELECT

    log(f"📦 mapa de produtos carregado: {len(mapa)}")
    return mapa


def limpar_tabela_total(tabela: str):
    log(f"🧹 limpando tabela {tabela}")
    while True:
        resp = executar_supabase_com_retry(
            lambda: supabase.table(tabela).select("id").limit(LOTE_SELECT).execute(),
            descricao=f"select ids {tabela}"
        )
        rows = resp.data or []
        if not rows:
            break

        ids = [row["id"] for row in rows if row.get("id") is not None]
        if not ids:
            break

        for i in range(0, len(ids), LOTE_DELETE):
            lote_ids = ids[i:i + LOTE_DELETE]
            executar_supabase_com_retry(
                lambda lote_ids=lote_ids: supabase.table(tabela).delete().in_("id", lote_ids).execute(),
                descricao=f"delete {tabela} lote"
            )

        log(f"🧹 {tabela}: removidos {len(ids)} registros neste ciclo")

    log(f"✅ tabela {tabela} limpa")


# =========================
# TRATAMENTO VENDAS
# =========================
def tratar_vendas(lista, token: str, mapa_produtos: dict):
    vendas_resultado = []
    itens_resultado = []

    total = len(lista)
    for idx, item in enumerate(lista, start=1):
        codigo = item.get("codigo") or item.get("id")
        if not codigo:
            continue

        detalhe = get_detalhe("vendas", codigo, token)
        if not detalhe:
            continue

        venda_id = to_str(detalhe.get("codigo") or detalhe.get("id"))
        data_venda = to_str(detalhe.get("dtVenda"))[:10] if detalhe.get("dtVenda") else None

        vendas_resultado.append({
            "id_origem": venda_id,
            "data_venda": data_venda,
            "numero": to_str(detalhe.get("numDoc") or detalhe.get("numero")),
            "cliente_id": to_str(detalhe.get("codContato")),
            "cliente_nome": detalhe.get("nomeContato") or detalhe.get("cliente_nome") or "Cliente não identificado",
            "valor_total": to_float(detalhe.get("valorTotal") or detalhe.get("valor_total") or detalhe.get("valor")),
            "desconto": to_float(detalhe.get("desconto")),
            "acrescimo": to_float(detalhe.get("acrescimo")),
            "situacao": to_str(detalhe.get("situacao") or ""),
            "forma_pagamento": to_str(
                detalhe.get("nomeFormaPgto")
                or (detalhe.get("financeiros")[0].get("nomeFormaPgto") if detalhe.get("financeiros") else "")
                or ""
            ),
        })

        itens = detalhe.get("produtos") or []
        for item_venda in itens:
            produto_id = to_str(item_venda.get("codProduto"))
            quantidade = to_float(item_venda.get("quant") or item_venda.get("quantidade"))
            valor_unitario = to_float(item_venda.get("preco") or item_venda.get("valorUnitario"))
            item_id = to_str(item_venda.get("codigo") or f"{venda_id}_{produto_id}")

            produto_info = mapa_produtos.get(produto_id, {})

            itens_resultado.append({
                "id_origem": item_id,
                "venda_id": venda_id,
                "produto_id": produto_id,
                "produto_nome": item_venda.get("descricao") or produto_info.get("nome") or "",
                "categoria_id": produto_info.get("categoria_id"),
                "categoria_nome": produto_info.get("categoria_nome") or "MERCADO",
                "quantidade": quantidade,
                "valor_unitario": valor_unitario,
                "valor_total": quantidade * valor_unitario,
            })

        if idx % 100 == 0:
            log(f"🔄 vendas processadas: {idx}/{total}")

        time.sleep(0.02)

    return vendas_resultado, itens_resultado


# =========================
# MAIN
# =========================
def main():
    log("🚀 INICIOU SYNC VENDAS FULL OTIMIZADO")

    token = get_token()

    mapa_produtos = carregar_mapa_produtos()

    vendas_lista = get_data_paginado("vendas", token)
    vendas_tratadas, itens_tratados = tratar_vendas(vendas_lista, token, mapa_produtos)

    log(f"qtd vendas tratadas: {len(vendas_tratadas)}")
    log(f"qtd itens tratados: {len(itens_tratados)}")

    # limpa tudo e recria
    limpar_tabela_total("eg_venda_itens")
    limpar_tabela_total("eg_vendas")

    upsert_lote("eg_vendas", vendas_tratadas, on_conflict="id_origem")
    upsert_lote("eg_venda_itens", itens_tratados, on_conflict="id_origem")

    log("✅ FINALIZADO SYNC VENDAS FULL OTIMIZADO")


if __name__ == "__main__":
    main()
