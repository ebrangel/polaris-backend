"""Saída em CSV nas rotas de consulta (seção 2.3a).

O que se verifica aqui é o contrato HTTP do formato — o CSV em si é do
`test_csv_presenter.py`, e a negociação, do `test_content_negotiation.py`.
"""

import json
from urllib.parse import urlencode

from domain.models import Column, DataType, QueryRequest, QueryResult

PAYLOAD = {
    "schema": "vendas",
    "dimensions": ["sigla_uf"],
    "measures": ["valor_total", "quantidade"],
}

CSV_ESPERADO = (
    "sigla_uf,valor_total,quantidade\r\nSP,458320.5,1204\r\nRJ,212904.1,588\r\n"
)


def _resultado(query_id: str = "q_8f2a1c") -> QueryResult:
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


def test_post_com_format_csv(client, job_queue, financeiro):
    job_queue.default_result = _resultado()

    response = client.post("/v1/query?format=csv", json=PAYLOAD, headers=financeiro)

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8; header=present"
    assert response.text == CSV_ESPERADO


def test_post_negocia_por_accept(client, job_queue, financeiro):
    job_queue.default_result = _resultado()

    response = client.post(
        "/v1/query", json=PAYLOAD, headers={**financeiro, "Accept": "text/csv"}
    )

    assert response.status_code == 200
    assert response.text == CSV_ESPERADO


def test_format_explicito_vence_o_accept(client, job_queue, financeiro):
    job_queue.default_result = _resultado()

    response = client.post(
        "/v1/query?format=json", json=PAYLOAD, headers={**financeiro, "Accept": "text/csv"}
    )

    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["rows"] == [["SP", 458320.50, 1204], ["RJ", 212904.10, 588]]


def test_sem_format_continua_json(client, job_queue, financeiro):
    """O formato de saída é aditivo: quem não pede nada recebe a seção 2.3 como antes."""
    job_queue.default_result = _resultado()

    response = client.post("/v1/query", json=PAYLOAD, headers=financeiro)

    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["status"] == "completed"


def test_meta_da_secao_2_3_vai_para_os_headers(client, job_queue, financeiro):
    job_queue.default_result = _resultado()

    response = client.post("/v1/query?format=csv", json=PAYLOAD, headers=financeiro)

    assert response.headers["x-query-id"] == "q_8f2a1c"
    assert response.headers["x-dataset-used"] == "vendas_agregado_uf"
    assert response.headers["x-cached"] == "true"
    assert response.headers["x-execution-ms"] == "12"
    assert response.headers["x-row-count"] == "2"
    assert response.headers["content-disposition"] == (
        'attachment; filename="q_8f2a1c.csv"'
    )


def test_format_no_corpo_e_rejeitado(client, financeiro):
    """`extra="forbid"` no `QueryRequestModel`: formato é transporte, não vai no corpo."""
    response = client.post(
        "/v1/query", json={**PAYLOAD, "format": "csv"}, headers=financeiro
    )

    assert response.status_code == 422
    assert response.json()["type"] == "malformed_request"


# --- o formato não faz parte da consulta --------------------------------------------------


def test_csv_e_json_compartilham_query_id(client, job_queue, financeiro):
    """O ponto central do desenho: `format` não entra no `QueryRequest`, então a mesma
    consulta em JSON e em CSV tem um `query_id` só e é enfileirada com o mesmo payload —
    a deduplicação por `query_id` (e o cache, gravado pelo worker) decorrem disso."""
    job_queue.default_result = _resultado()

    em_json = client.post("/v1/query", json=PAYLOAD, headers=financeiro)
    em_csv = client.post("/v1/query?format=csv", json=PAYLOAD, headers=financeiro)

    assert em_csv.headers["x-query-id"] == em_json.json()["query_id"]
    assert job_queue.calls[0][0].query_id == job_queue.calls[1][0].query_id


# --- GET /v1/query ------------------------------------------------------------------------


def test_get_com_parametros_planos(client, job_queue, financeiro):
    job_queue.default_result = _resultado()

    response = client.get(
        "/v1/query?schema=vendas&dimensions=sigla_uf&measures=valor_total&format=csv",
        headers=financeiro,
    )

    assert response.status_code == 200
    assert response.text == CSV_ESPERADO


def test_get_com_query_json_tambem_aceita_format(client, job_queue, financeiro):
    """"Se `query` estiver presente, os demais parâmetros são ignorados" vale para a
    consulta; `format` é transporte e continua valendo."""
    job_queue.default_result = _resultado()
    querystring = urlencode({"query": json.dumps(PAYLOAD), "format": "csv"})

    response = client.get(f"/v1/query?{querystring}", headers=financeiro)

    assert response.status_code == 200
    assert response.text == CSV_ESPERADO


# --- caminho assíncrono -------------------------------------------------------------------


def test_enfileiramento_responde_json_mesmo_pedindo_csv(client, financeiro):
    """`{query_id, status, poll_url}` não é tabela — o 202 sai em JSON sempre. Sem
    worker, a janela inline expira e a submissão devolve `processing`."""
    response = client.post("/v1/query?format=csv", json=PAYLOAD, headers=financeiro)

    assert response.status_code == 202
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["status"] == "processing"
    assert body["poll_url"] == f"/v1/query/{body['query_id']}"


async def test_status_em_processamento_responde_json(client, job_queue):
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")

    response = client.get(f"/v1/query/{request.query_id}?format=csv")

    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["status"] == "processing"


async def test_status_que_falhou_responde_json(client, job_queue):
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")
    job_queue.resolve(
        request.query_id, QueryResult.failed(request.query_id, error="query_timeout")
    )

    response = client.get(f"/v1/query/{request.query_id}?format=csv")

    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["error"] == "query_timeout"


async def test_download_do_resultado_de_uma_consulta_pesada(client, job_queue):
    """O fluxo completo do CSV grande: enfileira, acompanha em JSON, baixa em CSV."""
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")
    job_queue.resolve(request.query_id, _resultado(request.query_id))

    response = client.get(f"/v1/query/{request.query_id}?format=csv")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8; header=present"
    assert response.headers["content-disposition"] == (
        f'attachment; filename="{request.query_id}.csv"'
    )
    assert response.text == CSV_ESPERADO


# --- erros --------------------------------------------------------------------------------


def test_format_desconhecido_devolve_problem_json(client, financeiro):
    response = client.post("/v1/query?format=parquet", json=PAYLOAD, headers=financeiro)

    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"] == "invalid_format"
    assert body["fields"] == ["format"]
    assert "parquet" in body["detail"]


def test_erro_de_dominio_sai_em_json_mesmo_pedindo_csv(client, financeiro):
    """Corpo de erro nunca sai em CSV — não teria onde levar `type`/`title`/`detail`."""
    response = client.post(
        "/v1/query?format=csv",
        json={"schema": "vendas", "dimensions": ["campo_inexistente"]},
        headers=financeiro,
    )

    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["type"] == "unknown_field"


def test_query_id_desconhecido_sai_em_json_mesmo_pedindo_csv(client):
    response = client.get("/v1/query/q_000000?format=csv")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["type"] == "unknown_query"
