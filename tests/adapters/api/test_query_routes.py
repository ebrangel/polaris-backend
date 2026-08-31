"""`POST /v1/query` e `GET /v1/query` — as três formas de submeter a mesma consulta
(seções 2.2 e 2.2a) e o formato de resposta da seção 2.3.
"""

import json
from datetime import date
from decimal import Decimal
from urllib.parse import urlencode

from domain.models import Column, DataType, QueryResult

#: O corpo exato da seção 2.2.
PAYLOAD_SECAO_2_2 = {
    "schema": "vendas",
    "dimensions": ["sigla_uf"],
    "measures": ["valor_total", "quantidade"],
    "filters": [{"field": "sigla_uf", "operator": "in", "value": ["SP", "RJ"]}],
    "order_by": [{"field": "valor_total", "direction": "desc"}],
    "limit": 100,
    "offset": 0,
}


def _resultado_da_secao_2_3(query_id: str) -> QueryResult:
    """As linhas e colunas do exemplo de resposta da seção 2.3."""
    return QueryResult.completed(
        query_id=query_id,
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER, format="currency"),
            Column(field="quantidade", type=DataType.NUMBER),
        ),
        rows=(("SP", 458320.50, 1204), ("RJ", 212904.10, 588)),
        dataset_used="vendas_agregado_uf",
        cached=True,
        execution_ms=12,
    )


# --- POST /v1/query -----------------------------------------------------------------------


def test_post_devolve_o_formato_da_secao_2_3(client, executor, financeiro):
    executor.result = _resultado_da_secao_2_3("q_8f2a1c")

    response = client.post("/v1/query", json=PAYLOAD_SECAO_2_2, headers=financeiro)

    assert response.status_code == 200
    assert response.json() == {
        "query_id": "q_8f2a1c",
        "status": "completed",
        "columns": [
            {"field": "sigla_uf", "type": "string"},
            {"field": "valor_total", "type": "number", "format": "currency"},
            {"field": "quantidade", "type": "number"},
        ],
        "rows": [["SP", 458320.50, 1204], ["RJ", 212904.10, 588]],
        "meta": {
            "row_count": 2,
            "cached": True,
            "execution_ms": 12,
            "dataset_used": "vendas_agregado_uf",
        },
    }


def test_coluna_sem_format_omite_a_chave(client, executor, financeiro):
    """No exemplo da seção 2.3, `quantidade` não tem `format` — a chave não aparece."""
    executor.result = _resultado_da_secao_2_3("q_8f2a1c")

    colunas = client.post("/v1/query", json=PAYLOAD_SECAO_2_2, headers=financeiro).json()[
        "columns"
    ]

    assert "format" not in colunas[0]
    assert "format" not in colunas[2]
    assert colunas[1]["format"] == "currency"


def test_tipos_do_driver_sao_serializados(client, executor, financeiro):
    """Os routers devolvem `JSONResponse` já montada (para controlar 200 vs. 202), o
    que contorna a serialização automática do FastAPI. Uma coluna `numeric` do Postgres
    chega como `Decimal` e uma `date` como `datetime.date` — sem conversão explícita a
    resposta quebraria com `TypeError`. `Decimal` vira string para não perder precisão."""
    executor.result = QueryResult.completed(
        query_id="q_tipos",
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER, format="currency"),
        ),
        rows=((date(2024, 1, 31), Decimal("458320.50")),),
        dataset_used="vendas_agregado_uf",
    )

    response = client.post("/v1/query", json=PAYLOAD_SECAO_2_2, headers=financeiro)

    assert response.status_code == 200
    assert response.json()["rows"] == [["2024-01-31", "458320.50"]]


def test_post_chega_no_use_case_com_a_requisicao_traduzida(client, executor, financeiro):
    client.post("/v1/query", json=PAYLOAD_SECAO_2_2, headers=financeiro)

    _, domain_request, _, _ = executor.calls[0]
    assert domain_request.schema == "vendas"
    assert domain_request.dimensions == ("sigla_uf",)
    assert domain_request.measures == ("valor_total", "quantidade")
    assert domain_request.filters[0].value == ("SP", "RJ")
    assert domain_request.order_by[0].direction.value == "desc"
    assert domain_request.limit == 100


def test_resposta_assincrona_da_secao_2_4_usa_202_e_poll_url(client, executor, financeiro):
    """O `ExecuteQuery` do Marco 4 só devolve `completed`; a fila é do Marco 7. O
    presenter já cobre o caso para que o enfileiramento não precise mexer no router."""
    executor.result = QueryResult.processing("q_9d31be")

    response = client.post("/v1/query", json=PAYLOAD_SECAO_2_2, headers=financeiro)

    assert response.status_code == 202
    assert response.json() == {
        "query_id": "q_9d31be",
        "status": "processing",
        "poll_url": "/v1/query/q_9d31be",
    }


# --- GET /v1/query, opção A: query=<json> ---------------------------------------------------


def test_get_com_query_json_e_identico_ao_post(client, executor, financeiro):
    executor.result = _resultado_da_secao_2_3("q_8f2a1c")
    do_post = client.post("/v1/query", json=PAYLOAD_SECAO_2_2, headers=financeiro).json()

    querystring = urlencode({"query": json.dumps(PAYLOAD_SECAO_2_2)})
    do_get = client.get(f"/v1/query?{querystring}", headers=financeiro)

    assert do_get.status_code == 200
    assert do_get.json() == do_post


def test_query_json_tem_precedencia_sobre_os_parametros_planos(client, executor, financeiro):
    """"Se `query` estiver presente, os demais parâmetros são ignorados." (seção 2.2a)"""
    querystring = urlencode(
        {"query": json.dumps({"schema": "vendas", "dimensions": ["sigla_uf"]}), "limit": "7"}
    )

    client.get(f"/v1/query?{querystring}", headers=financeiro)

    _, domain_request, _, _ = executor.calls[0]
    # O `limit=7` da querystring foi ignorado; o que chega no executor é o teto do
    # schema `vendas` (seção 2.6), aplicado a toda requisição que não traz `limit`.
    assert domain_request.limit == 50_000
    assert domain_request.dimensions == ("sigla_uf",)


# --- GET /v1/query, opção B: parâmetros planos ----------------------------------------------


def test_get_com_parametros_planos_converge_para_a_mesma_resposta(client, executor, financeiro):
    executor.result = _resultado_da_secao_2_3("q_8f2a1c")
    do_post = client.post("/v1/query", json=PAYLOAD_SECAO_2_2, headers=financeiro).json()

    do_get = client.get(
        "/v1/query"
        "?schema=vendas&dimensions=sigla_uf&measures=valor_total,quantidade"
        "&filter[sigla_uf][in]=SP,RJ&order_by=valor_total.desc&limit=100&offset=0",
        headers=financeiro,
    )

    assert do_get.status_code == 200
    assert do_get.json() == do_post


def test_o_exemplo_literal_da_secao_2_2a(client, executor, financeiro):
    """A URL exata do documento — `dimensions=sigla_uf,cargo` leva o resolvedor ao
    `vendas_detalhado`, mas isso é invisível daqui: só se confere que a requisição
    chegou traduzida."""
    response = client.get(
        "/v1/query"
        "?schema=vendas&dimensions=sigla_uf,cargo&measures=valor_total"
        "&filter[sigla_uf][in]=SP,RJ&order_by=valor_total.desc&limit=100&offset=0",
        headers=financeiro,
    )

    assert response.status_code == 200
    dataset, domain_request, _, _ = executor.calls[0]
    assert dataset.name == "vendas_detalhado"
    assert domain_request.dimensions == ("sigla_uf", "cargo")
    assert domain_request.filters[0].value == ("SP", "RJ")


def test_get_sem_schema(client, financeiro):
    response = client.get("/v1/query?dimensions=sigla_uf", headers=financeiro)

    assert response.status_code == 422
    assert response.json()["type"] == "unknown_field"


def test_get_com_json_invalido_em_query(client, financeiro):
    response = client.get("/v1/query?query=%7Bnao-e-json", headers=financeiro)

    assert response.status_code == 422
    body = response.json()
    assert body["type"] == "malformed_request"
    assert body["fields"] == ["query"]
