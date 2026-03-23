import os
import time
import requests
from datetime import datetime

# =========================
# CONFIGURAÇÕES
# =========================
EGESTOR_PERSONAL_TOKEN = os.getenv("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHAiOiIxMGQxNzEzNGJiNmNkMTlkNGQ3YmNkYzgwNTNlNjRjMSIsInN1YmRvbWluaW8iOiJwYWRhcmlha3JvbmluZyIsImNsaWVudCI6IjY3Nzg4NmMxNDdkZWRiNWI3OTI2M2ZjYTUzZDMzNWY1M2Q1YTRmNzMiLCJjcmVhdGVkIjoxNzcyOTk5MTI0fQ==.dUAdBdESSI7mtkTvMeBRRXPOo5dWHIjpZRWHZCVuk6I", "")
SUPABASE_URL = os.getenv("https://grusczwscturplevobsv.supabase.co", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdydXNjendzY3R1cnBsZXZvYnN2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzcwNjQ2MCwiZXhwIjoyMDg5MjgyNDYwfQ.lpOXnPdkUI9sUWY28maWeJaKvfvu3rw780AIjwP_BSg", "")

# carga completa desde o início
DATA_INICIO_VENDAS = os.getenv("DATA_INICIO_VENDAS", "2025-10-27")

LOTE_ITENS_VENDA = int(os.getenv("LOTE_ITENS_VENDA", "100"))

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
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


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


def data_inicio_carga() -> str:
    return DATA_INICIO_VENDAS


# =========================
# AUTH EGESTOR
# =========================
def get_token() -> str:
    url = "https://api.egestor.com.br/api/oauth/access_token"
    payload = {
        "grant_type": "personal",
        "personal_token": EGESTOR_PERSONAL_TOKEN,
    }

    r = requests.post(url, json=payload, timeout=60)
    log(f"AUTH status: {r.status_code}")
    if r.status_code != 200:
        log(r.text[:500])
    r.raise_for_status()

    body = r.json()
    access_token = body.get("access_token")
    if not access_token:
        raise Exception("Não veio access_token do eGestor.")
    return access_token


# =========================
# API EGESTOR
# =========================
def get_data(endpoint: str, token: str):
    url = f"https://api.egestor.com.br/api/v1/{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers, timeout=120)
    log(f"{endpoint} status: {r.status_code}")

    if r.status_code != 200:
        log(r.text[:1000])

    r.raise_for_status()

    body = r.json()
    data = body.get("data", [])

    log(f"{endpoint}: {len(data)} registros")
    if data:
        log(f"exemplo {endpoint}: {data[0]}")

    return data


def get_data_paginado(endpoint: str, token: str):
    all_data = []
    page = 1
    tentativas_429 = 0
    token_atual = token

    while True:
        url = f"https://api.egestor.com.br/api/v1/{endpoint}?page={page}"
        headers = {"Authorization": f"Bearer {token_atual}"}

        r = requests.get(url, headers=headers, timeout=120)
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
        time.sleep(0.4)

    log(f"TOTAL {endpoint}: {len(all_data)} registros")
    if all_data:
        log(f"{endpoint} primeiro bruto: {all_data[0]}")
        log(f"{endpoint} último bruto: {all_data[-1]}")

    return all_data


def get_detalhe(endpoint: str, codigo, token: str):
    token_atual = token

    while True:
        url = f"https://api.egestor.com.br/api/v1/{endpoint}/{codigo}"
        headers = {"Authorization": f"Bearer {token_atual}"}

        r = requests.get(url, headers=headers, timeout=120)
        log(f"detalhe {endpoint} {codigo} status: {r.status_code}")

        if r.status_code == 429:
            log(f"detalhe {endpoint} {codigo}: limite da API, aguardando 10s")
            time.sleep(10)
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

    r = requests.post(url, headers=headers, json=dados, timeout=120)
    log(f"{tabela} supabase status: {r.status_code}")
    if r.status_code >= 300:
        log(r.text[:1000])
    r.raise_for_status()


def deletar_vendas_desde(data_inicio: str) -> None:
    url = f"{SUPABASE_URL}/rest/v1/eg_vendas?data_venda=gte.{data_inicio}"
    r = requests.delete(url, headers=supabase_headers(), timeout=120)
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

        r = requests.delete(url, headers=headers, timeout=120)
        log(f"delete eg_venda_itens lote {i} até {i + bloco}: {r.status_code}")
        if r.text:
            log(r.text[:300])
        time.sleep(0.3)


# =========================
# CATEGORIAS
# =========================
def tratar_categorias(lista):
    resultado = []

    for item in lista:
        resultado.append({
            "id_origem": to_str(item.get("codigo") or item.get("id")),
            "nome": item.get("nome") or item.get("descricao"),
            "situacao": item.get("situacao") or "OK",
        })

    if resultado:
        log(f"categoria tratada: {resultado[0]}")
    return resultado


def montar_mapa_categorias(lista_categorias):
    mapa = {}

    for item in lista_categorias:
        codigo = item.get("codigo") or item.get("id")
        nome = item.get("nome") or item.get("descricao")
        if codigo is not None:
            mapa[str(codigo)] = nome

    log(f"mapa categorias com {len(mapa)} itens")
    return mapa


# =========================
# PRODUTOS
# =========================
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

    if resultado:
        log(f"produto tratado: {resultado[0]}")
    return resultado


def montar_mapa_produtos(produtos_tratados):
    mapa = {}

    for p in produtos_tratados:
        mapa[str(p["id_origem"])] = {
            "nome": p.get("nome"),
            "categoria_id": p.get("categoria_id"),
            "categoria_nome": p.get("categoria_nome"),
            "custo": p.get("custo", 0),
        }

    log(f"mapa produtos montado com {len(mapa)} itens")
    return mapa


# =========================
# VENDAS
# =========================
def tratar_vendas(lista, data_inicio: str, token: str):
    resultado = []
    data_inicio_dt = datetime.strptime(data_inicio, "%Y-%m-%d")
    datas_encontradas = []

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

        datas_encontradas.append(data_obj)

        if data_obj < data_inicio_dt:
            continue

        codigo_venda = item.get("codigo") or item.get("id")
        if not codigo_venda:
            continue

        detalhe = get_detalhe("vendas", codigo_venda, token)
        if not detalhe:
            continue

        resultado.append({
            "id_origem": to_str(detalhe.get("codigo") or detalhe.get("id")),
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

        time.sleep(0.2)

    if datas_encontradas:
        log(f"menor data vinda da API: {min(datas_encontradas).strftime('%Y-%m-%d')}")
        log(f"maior data vinda da API: {max(datas_encontradas).strftime('%Y-%m-%d')}")

    if resultado:
        log(f"primeira venda tratada: {resultado[0]}")
        log(f"última venda tratada: {resultado[-1]}")

    return resultado


# =========================
# ITENS DE VENDA
# =========================
def tratar_itens_de_venda(vendas, mapa_produtos, token: str):
    resultado = []

    for venda in vendas:
        codigo_venda = venda.get("id_origem") or venda.get("id") or venda.get("codigo")
        if not codigo_venda:
            continue

        detalhe = get_detalhe("vendas", codigo_venda, token)
        if not detalhe:
            continue

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

        time.sleep(0.2)

    if resultado:
        log(f"primeiro item venda tratado: {resultado[0]}")

    return resultado


# =========================
# RECEBIMENTOS
# =========================
def tratar_recebimentos(lista, token: str):
    resultado = []

    for item in lista:
        codigo = item.get("codigo") or item.get("id")
        if not codigo:
            continue

        detalhe = get_detalhe("recebimentos", codigo, token)
        if not detalhe:
            continue

        registro = {
            "id_origem": to_str(detalhe.get("codigo") or detalhe.get("id")),
            "data": to_str(
                detalhe.get("dtVenc")
                or detalhe.get("dtRec")
                or detalhe.get("dtPgto")
                or detalhe.get("data")
            )[:10],
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
        }

        resultado.append(registro)
        time.sleep(0.2)

    if resultado:
        log(f"primeiro recebimento tratado: {resultado[0]}")

    return resultado


# =========================
# PAGAMENTOS
# =========================
def tratar_pagamentos(lista, token: str):
    resultado = []

    for item in lista:
        codigo = item.get("codigo") or item.get("id")
        if not codigo:
            continue

        detalhe = get_detalhe("pagamentos", codigo, token)
        if not detalhe:
            continue

        registro = {
            "id_origem": to_str(detalhe.get("codigo") or detalhe.get("id")),
            "data": to_str(
                detalhe.get("dtVenc")
                or detalhe.get("dtPgto")
                or detalhe.get("data")
            )[:10],
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
        }

        resultado.append(registro)
        time.sleep(0.2)

    if resultado:
        log(f"primeiro pagamento tratado: {resultado[0]}")

    return resultado


# =========================
# PLANO DE CONTAS
# =========================
def tratar_plano_contas(lista):
    resultado = []

    for item in lista:
        resultado.append({
            "id_origem": to_str(item.get("codigo") or item.get("id")),
            "nome": item.get("nome") or "",
            "tipo": to_str(item.get("tipo") or ""),
            "grupo": to_str(item.get("codPai") or ""),
            "situacao": "OK",
        })

    if resultado:
        log(f"primeiro plano de contas tratado: {resultado[0]}")

    return resultado


# =========================
# MAIN
# =========================
def main():
    log("INICIOU CARGA COMPLETA DESDE O INÍCIO")

    token = get_token()

    # 1) categorias
    categorias = get_data("categorias", token)
    categorias_tratadas = tratar_categorias(categorias)
    enviar_supabase("eg_categorias", categorias_tratadas)
    mapa_categorias = montar_mapa_categorias(categorias)

    # 2) produtos
    produtos = get_data_paginado("produtos", token)
    produtos_tratados = tratar_produtos(produtos, mapa_categorias)
    enviar_supabase("eg_produtos", produtos_tratados)
    mapa_produtos = montar_mapa_produtos(produtos_tratados)

    # 3) plano de contas
    plano_contas = get_data_paginado("planoContas", token)
    plano_contas_tratados = tratar_plano_contas(plano_contas)
    enviar_supabase("eg_plano_contas", plano_contas_tratados)

    # 4) vendas desde o início
    data_inicio = data_inicio_carga()
    log(f"Buscando vendas desde: {data_inicio}")

    vendas = get_data_paginado("vendas", token)
    vendas_tratadas = tratar_vendas(vendas, data_inicio, token)
    log(f"qtd vendas tratadas: {len(vendas_tratadas)}")

    deletar_vendas_desde(data_inicio)
    enviar_supabase("eg_vendas", vendas_tratadas)

    # 5) itens de venda desde o início
    ids_venda = [v["id_origem"] for v in vendas_tratadas]
    deletar_itens_venda_ids(ids_venda)

    for i in range(0, len(vendas_tratadas), LOTE_ITENS_VENDA):
        bloco = vendas_tratadas[i:i + LOTE_ITENS_VENDA]
        log(f"Processando vendas {i} até {i + LOTE_ITENS_VENDA}")

        itens = tratar_itens_de_venda(bloco, mapa_produtos, token)
        log(f"qtd itens lote: {len(itens)}")

        enviar_supabase("eg_venda_itens", itens)
        time.sleep(2)

    # 6) recebimentos completos
    recebimentos = get_data_paginado("recebimentos", token)
    recebimentos_tratados = tratar_recebimentos(recebimentos, token)
    enviar_supabase("eg_recebimentos", recebimentos_tratados)

    # 7) pagamentos completos
    pagamentos = get_data_paginado("pagamentos", token)
    pagamentos_tratados = tratar_pagamentos(pagamentos, token)
    enviar_supabase("eg_pagamentos", pagamentos_tratados)

    log("FINALIZADO CARGA COMPLETA DESDE O INÍCIO")


if __name__ == "__main__":
    main()
