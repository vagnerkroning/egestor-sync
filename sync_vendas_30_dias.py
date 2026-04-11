import os
import time
import requests
from datetime import datetime, timedelta
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout

# =========================
# CONFIG
# =========================
EGESTOR_PERSONAL_TOKEN = os.getenv("EGESTOR_PERSONAL_TOKEN", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

DIAS = int(os.getenv("DIAS_VENDAS", "30"))

if not EGESTOR_PERSONAL_TOKEN:
    raise ValueError("EGESTOR_PERSONAL_TOKEN não definido.")
if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL não definido.")
if not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("SUPABASE_SERVICE_ROLE_KEY não definido.")


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


def data_inicio() -> str:
    return (datetime.now() - timedelta(days=DIAS)).strftime("%Y-%m-%d")


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
            elif method == "DELETE":
                r = requests.delete(url, headers=headers, timeout=timeout)
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
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


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


def buscar_produto(codigo, token: str):
    return get_detalhe("produtos", codigo, token)


def buscar_categoria_nome(cod_categoria, token: str):
    if cod_categoria is None or cod_categoria == "":
        return None

    resp = get_detalhe("categorias", cod_categoria, token)
    if not resp:
        return None

    return resp.get("nome") or resp.get("descricao")


# =========================
# SUPABASE REST
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


def deletar_por_data(tabela: str, campo_data: str, data_inicio_ref: str) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{tabela}?{campo_data}=gte.{data_inicio_ref}"
    r = request_com_retry("DELETE", url, headers=supabase_headers(), timeout=120, tentativas=5)
    log(f"delete {tabela} desde {data_inicio_ref}: {r.status_code}")
    if r.text:
        log(r.text[:300])


def deletar_itens_por_vendas(venda_ids):
    if not venda_ids:
        log("eg_venda_itens: nenhum venda_id para excluir")
        return

    ids_texto = ",".join([to_str(v) for v in venda_ids if v is not None])
    if not ids_texto:
        return

    url = f"{SUPABASE_URL}/rest/v1/eg_venda_itens?venda_id=in.({ids_texto})"
    r = request_com_retry("DELETE", url, headers=supabase_headers(), timeout=120, tentativas=5)
    log(f"delete eg_venda_itens por venda_id: {r.status_code}")
    if r.text:
        log(r.text[:300])


# =========================
# TRATAMENTO VENDAS
# =========================
def tratar_vendas(lista, token: str, data_inicio_ref: str):
    vendas_resultado = []
    itens_resultado = []
    venda_ids_periodo = []

    data_inicio_dt = datetime.strptime(data_inicio_ref, "%Y-%m-%d")

    for item in lista:
        codigo = item.get("codigo") or item.get("id")
        if not codigo:
            continue

        detalhe = get_detalhe("vendas", codigo, token)
        if not detalhe:
            continue

        data_venda = to_str(detalhe.get("dtVenda"))[:10]
        try:
            data_obj = datetime.strptime(data_venda, "%Y-%m-%d")
        except Exception:
            continue

        if data_obj < data_inicio_dt:
            continue

        venda_id = to_str(detalhe.get("codigo") or detalhe.get("id"))
        venda_ids_periodo.append(venda_id)

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

            categoria_id = None
            categoria_nome = None

            if produto_id:
                produto = buscar_produto(produto_id, token)
                if produto:
                    categoria_id = to_str(produto.get("codCategoria"))
                    categoria_nome = buscar_categoria_nome(categoria_id, token)

            itens_resultado.append({
                "id_origem": item_id,
                "venda_id": venda_id,
                "produto_id": produto_id,
                "produto_nome": item_venda.get("descricao"),
                "categoria_id": categoria_id,
                "categoria_nome": categoria_nome if categoria_nome else "MERCADO",
                "quantidade": quantidade,
                "valor_unitario": valor_unitario,
                "valor_total": quantidade * valor_unitario,
            })

        time.sleep(0.05)

    return vendas_resultado, itens_resultado, venda_ids_periodo


# =========================
# MAIN
# =========================
def main():
    inicio = data_inicio()
    log(f"🚀 INICIOU SYNC VENDAS | últimos {DIAS} dias | desde {inicio}")

    token = get_token()

    vendas = get_data_paginado("vendas", token)
    vendas_tratadas, itens_tratados, venda_ids_periodo = tratar_vendas(vendas, token, inicio)

    log(f"qtd vendas tratadas: {len(vendas_tratadas)}")
    log(f"qtd itens tratados: {len(itens_tratados)}")

    # primeiro exclui itens das vendas do período
    deletar_itens_por_vendas(venda_ids_periodo)

    # depois exclui vendas do período
    deletar_por_data("eg_vendas", "data_venda", inicio)

    # grava vendas e itens novamente
    enviar_supabase("eg_vendas", vendas_tratadas)
    enviar_supabase("eg_venda_itens", itens_tratados)

    log("✅ FINALIZADO SYNC VENDAS")


if __name__ == "__main__":
    main()
