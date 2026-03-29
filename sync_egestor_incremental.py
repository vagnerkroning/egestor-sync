import os
import time
import requests
from datetime import datetime, timedelta
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout

# =========================
# CONFIGURAÇÕES
# =========================
EGESTOR_PERSONAL_TOKEN = os.getenv("EGESTOR_PERSONAL_TOKEN", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

DIAS_INCREMENTAL = int(os.getenv("DIAS_INCREMENTAL", "7"))
LOTE_ITENS_VENDA = int(os.getenv("LOTE_ITENS_VENDA", "500"))

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


def data_inicio_incremental() -> str:
    return (datetime.now() - timedelta(days=DIAS_INCREMENTAL)).strftime("%Y-%m-%d")


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
    return {"Authorization": f"Bearer {token}"}


# =========================
# API EGESTOR
# =========================
def get_data(endpoint: str, token: str):
    token_atual = token
    url = f"https://api.egestor.com.br/api/v1/{endpoint}"

    for _ in range(3):
        headers = egestor_headers(token_atual)
        r = request_com_retry("GET", url, headers=headers, timeout=120, tentativas=5)
        log(f"{endpoint} status: {r.status_code}")

        if r.status_code == 401:
            log(f"{endpoint}: token expirado, renovando")
            token_atual = get_token()
            time.sleep(1)
            continue

        if r.status_code != 200:
            log(r.text[:1000])
            r.raise_for_status()

        body = r.json()
        data = body.get("data", [])
        log(f"{endpoint}: {len(data)} registros")
        return data

    raise Exception(f"Falha ao buscar {endpoint} após renovar token.")


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


def deletar_vendas_desde(data_inicio: str) -> None:
    url = f"{SUPABASE_URL}/rest/v1/eg_vendas?data_venda=gte.{data_inicio}"
    r = request_com_retry("DELETE", url, headers=supabase_headers(), timeout=120, tentativas=5)
    log(f"delete eg_vendas desde {data_inicio}: {r.status_code}")
    if r.text:
        log(r.text[:300])


def deletar_itens_venda_ids(lista_ids_venda) -> None:
    if not lista_ids_venda:
        log("eg_venda_itens: nenhum venda_id para deletar")
        return

    bloco = 100
    headers = supabase_headers()

    for i in range(0, len(lista_ids_venda), bloco):
        parte = lista_ids_venda[i:i + bloco]
        ids = ",".join([f'"{x}"' for x in parte])
        url = f"{SUPABASE_URL}/rest/v1/eg_venda_itens?venda_id=in.({ids})"

        r = request_com_retry("DELETE", url, headers=headers, timeout=120, tentativas=5)
        log(f"delete eg_venda_itens lote {i} até {i + bloco}: {r.status_code}")
        if r.text:
            log(r.text[:300])
        time.sleep(0.05)


def deletar_por_data(tabela: str, campo_data: str, data_inicio: str) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{tabela}?{campo_data}=gte.{data_inicio}"
    r = request_com_retry("DELETE", url, headers=supabase_headers(), timeout=120, tentativas=5)
    log(f"delete {tabela} desde {data_inicio}: {r.status_code}")
    if r.text:
        log(r.text[:300])


# =========================
# CATEGORIAS / PRODUTOS
# =========================
def montar_mapa_categorias(lista_categorias):
    mapa = {}
    for item in lista_categorias:
        codigo = item.get("codigo") or item.get("id")
        nome = item.get("nome") or item.get("descricao")
        if codigo is not None:
            mapa[str(codigo)] = nome
    return mapa


def montar_mapa_produtos(produtos_tratados):
    mapa = {}
    for p in produtos_tratados:
        mapa[str(p["id_origem"])] = {
            "nome": p.get("nome"),
            "categoria_id": p.get("categoria_id"),
            "categoria_nome": p.get("categoria_nome"),
            "custo": p.get("custo", 0),
        }
    return mapa


def tratar_produtos(lista, mapa_categorias):
    resultado = []

    for item in lista:
        cod_categoria = item.get("codCategoria")
        cod_categoria_str = str(cod_categoria) if cod_categoria is not None else None
        nome_categoria = mapa_categorias.get(cod_categoria_str)

        resultado.append({
            "id_origem": to_str(item.get("codigo") or item.get("id")),
            "codigo": to_str(item.get("codigo") or ""),
            "nome": item.get("nome") or item.get("descricao"),
            "categoria_id": cod_categoria_str,
            "categoria_nome": nome_categoria,
            "unidade": item.get("unidadeTributada") or item.get("unidade"),
            "valor_venda": to_float(item.get("precoVenda") or item.get("valor_venda")),
            "custo": to_float(item.get("precoCusto") or item.get("custo")),
            "estoque": to_float(item.get("estoque")),
            "situacao": "OK",
        })

    return resultado


# =========================
# VENDAS + ITENS
# =========================
def tratar_vendas_e_detalhes(lista, data_inicio: str, token: str):
    vendas_tratadas = []
    detalhes_vendas = {}
    data_inicio_dt = datetime.strptime(data_inicio, "%Y-%m-%d")

    for item in lista:
        data_str = (
            item.get("dtVenda")
            or item.get("data")
            or item.get("createdAt")
            or item.get("created_at")
        )

        if not data_str:
            continue

        try:
            data_obj = datetime.strptime(str(data_str)[:10], "%Y-%m-%d")
        except Exception:
            continue

        if data_obj < data_inicio_dt:
            continue

        codigo_venda = item.get("codigo") or item.get("id")
        if not codigo_venda:
            continue

        detalhe = get_detalhe("vendas", codigo_venda, token)
        if not detalhe:
            continue

        id_venda = to_str(detalhe.get("codigo") or detalhe.get("id"))
        detalhes_vendas[id_venda] = detalhe

        vendas_tratadas.append({
            "id_origem": id_venda,
            "data_venda": to_str(detalhe.get("dtVenda") or data_str)[:10],
            "numero": to_str(detalhe.get("numDoc") or detalhe.get("numero") or ""),
            "cliente_id": to_str(detalhe.get("codContato") or ""),
            "cliente_nome": (
                detalhe.get("nomeContato")
                or detalhe.get("cliente_nome")
                or "Cliente não identificado"
            ),
            "valor_total": to_float(
                detalhe.get("valorTotal")
                or detalhe.get("valor_total")
                or detalhe.get("valor")
            ),
            "desconto": to_float(detalhe.get("desconto")),
            "acrescimo": to_float(detalhe.get("acrescimo")),
            "situacao": to_str(detalhe.get("situacao") or "OK"),
            "forma_pagamento": to_str(
                detalhe.get("nomeFormaPgto")
                or detalhe.get("forma_pagamento")
                or ""
            ),
        })

        time.sleep(0.01)

    return vendas_tratadas, detalhes_vendas


def tratar_itens_de_venda_por_detalhes(detalhes_vendas, mapa_produtos):
    resultado = []

    for codigo_venda, detalhe in detalhes_vendas.items():
        produtos = detalhe.get("produtos") or []

        for item in produtos:
            cod_produto = item.get("codProduto")
            produto_info = mapa_produtos.get(str(cod_produto), {})

            item_codigo = item.get("codigo") or f"{codigo_venda}_{cod_produto}"
            quantidade = to_float(item.get("quant") or item.get("quantidade"))
            valor_unitario = to_float(item.get("preco") or item.get("valorUnitario"))

            resultado.append({
                "id_origem": str(item_codigo),
                "venda_id": str(codigo_venda),
                "produto_id": str(cod_produto) if cod_produto is not None else "",
                "produto_nome": item.get("descricao") or produto_info.get("nome") or "",
                "categoria_id": produto_info.get("categoria_id"),
                "categoria_nome": produto_info.get("categoria_nome"),
                "quantidade": quantidade,
                "valor_unitario": valor_unitario,
                "valor_total": quantidade * valor_unitario,
            })

    return resultado


# =========================
# RECEBIMENTOS / PAGAMENTOS
# =========================
def tratar_recebimentos(lista, token: str, data_inicio: str):
    resultado = []
    data_inicio_dt = datetime.strptime(data_inicio, "%Y-%m-%d")

    for item in lista:
        codigo = item.get("codigo") or item.get("id")
        if not codigo:
            continue

        detalhe = get_detalhe("recebimentos", codigo, token)
        if not detalhe:
            continue

        data_str = to_str(
            detalhe.get("dtVenc")
            or detalhe.get("dtRec")
            or detalhe.get("dtPgto")
            or detalhe.get("data")
        )[:10]

        try:
            data_obj = datetime.strptime(data_str, "%Y-%m-%d")
        except Exception:
            continue

        if data_obj < data_inicio_dt:
            continue

        resultado.append({
            "id_origem": to_str(detalhe.get("codigo") or detalhe.get("id")),
            "data": data_str,
            "contato_id": to_str(detalhe.get("codContato") or ""),
            "contato_nome": (
                detalhe.get("nomeContato")
                or detalhe.get("contatoNome")
                or item.get("nomeContato")
                or item.get("contatoNome")
                or "Não identificado"
            ),
            "plano_conta_id": to_str(detalhe.get("codPlanoContas") or ""),
            "plano_conta_nome": "",
            "valor": to_float(detalhe.get("valor")),
            "situacao": to_str(detalhe.get("situacao") or ""),
            "origem": "recebimento",
        })

        time.sleep(0.01)

    return resultado


def tratar_pagamentos(lista, token: str, data_inicio: str):
    resultado = []
    data_inicio_dt = datetime.strptime(data_inicio, "%Y-%m-%d")

    for item in lista:
        codigo = item.get("codigo") or item.get("id")
        if not codigo:
            continue

        detalhe = get_detalhe("pagamentos", codigo, token)
        if not detalhe:
            continue

        data_str = to_str(
            detalhe.get("dtVenc")
            or detalhe.get("dtPgto")
            or detalhe.get("data")
        )[:10]

        try:
            data_obj = datetime.strptime(data_str, "%Y-%m-%d")
        except Exception:
            continue

        if data_obj < data_inicio_dt:
            continue

        resultado.append({
            "id_origem": to_str(detalhe.get("codigo") or detalhe.get("id")),
            "data": data_str,
            "contato_id": to_str(detalhe.get("codContato") or ""),
            "contato_nome": (
                detalhe.get("nomeContato")
                or detalhe.get("contatoNome")
                or item.get("nomeContato")
                or item.get("contatoNome")
                or "Não identificado"
            ),
            "plano_conta_id": to_str(detalhe.get("codPlanoContas") or ""),
            "plano_conta_nome": "",
            "valor": to_float(detalhe.get("valor")),
            "situacao": to_str(detalhe.get("situacao") or ""),
            "origem": "pagamento",
        })

        time.sleep(0.01)

    return resultado


# =========================
# MAIN
# =========================
def main():
    data_inicio = data_inicio_incremental()
    log(f"INICIOU INCREMENTAL | últimos {DIAS_INCREMENTAL} dias | desde {data_inicio}")

    token = get_token()

    # categorias e produtos atualizados para manter nomes/categorias alinhados
    categorias = get_data("categorias", token)
    mapa_categorias = montar_mapa_categorias(categorias)

    produtos = get_data_paginado("produtos", token)
    produtos_tratados = tratar_produtos(produtos, mapa_categorias)
    enviar_supabase("eg_produtos", produtos_tratados)
    mapa_produtos = montar_mapa_produtos(produtos_tratados)

    # vendas últimos 7 dias
    vendas = get_data_paginado("vendas", token)
    vendas_tratadas, detalhes_vendas = tratar_vendas_e_detalhes(vendas, data_inicio, token)
    log(f"qtd vendas incremental: {len(vendas_tratadas)}")

    deletar_vendas_desde(data_inicio)
    enviar_supabase("eg_vendas", vendas_tratadas)

    ids_venda = [v["id_origem"] for v in vendas_tratadas]
    deletar_itens_venda_ids(ids_venda)

    ids_em_ordem = [v["id_origem"] for v in vendas_tratadas]
    for i in range(0, len(ids_em_ordem), LOTE_ITENS_VENDA):
        lote_ids = ids_em_ordem[i:i + LOTE_ITENS_VENDA]
        log(f"Processando lote de itens {i} até {i + LOTE_ITENS_VENDA}")

        detalhes_lote = {k: detalhes_vendas[k] for k in lote_ids if k in detalhes_vendas}
        itens = tratar_itens_de_venda_por_detalhes(detalhes_lote, mapa_produtos)
        log(f"qtd itens lote: {len(itens)}")
        enviar_supabase("eg_venda_itens", itens)
        time.sleep(0.05)

    # recebimentos últimos 7 dias
    recebimentos = get_data_paginado("recebimentos", token)
    recebimentos_tratados = tratar_recebimentos(recebimentos, token, data_inicio)
    log(f"qtd recebimentos incremental: {len(recebimentos_tratados)}")
    deletar_por_data("eg_recebimentos", "data", data_inicio)
    enviar_supabase("eg_recebimentos", recebimentos_tratados)

    # pagamentos últimos 7 dias
    pagamentos = get_data_paginado("pagamentos", token)
    pagamentos_tratados = tratar_pagamentos(pagamentos, token, data_inicio)
    log(f"qtd pagamentos incremental: {len(pagamentos_tratados)}")
    deletar_por_data("eg_pagamentos", "data", data_inicio)
    enviar_supabase("eg_pagamentos", pagamentos_tratados)

    log("FINALIZADO INCREMENTAL DOS ÚLTIMOS 7 DIAS")


if __name__ == "__main__":
    main()
