"""`GET /v1/query/{query_id}` — acompanhamento de consulta assíncrona (seção 2.4).

O enfileiramento em si é do Marco 7; aqui o endpoint lê o `JobQueue` diretamente, então
já dá para exercitar os três estados usando o fake do Marco 2.
"""

from domain.models import Column, DataType, QueryRequest, QueryResult


def _request() -> QueryRequest:
    return QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))


async def test_consulta_em_processamento(client, job_queue):
    request = _request()
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")

    response = client.get(f"/v1/query/{request.query_id}")

    assert response.status_code == 200
    assert response.json() == {
        "query_id": request.query_id,
        "status": "processing",
        "poll_url": f"/v1/query/{request.query_id}",
    }


async def test_consulta_concluida_assume_o_formato_da_secao_2_3(client, job_queue):
    """"O cliente então consulta GET /v1/query/{query_id} até status: "completed",
    quando a resposta assume o formato da seção 2.3." """
    request = _request()
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")
    job_queue.resolve(
        request.query_id,
        QueryResult.completed(
            query_id=request.query_id,
            columns=(
                Column(field="sigla_uf", type=DataType.STRING),
                Column(field="valor_total", type=DataType.NUMBER, format="currency"),
            ),
            rows=(("SP", 458320.50),),
            dataset_used="vendas_agregado_uf",
            execution_ms=1200,
        ),
    )

    body = client.get(f"/v1/query/{request.query_id}").json()

    assert body["status"] == "completed"
    assert body["rows"] == [["SP", 458320.50]]
    assert body["meta"]["dataset_used"] == "vendas_agregado_uf"
    assert body["columns"][1]["format"] == "currency"


async def test_consulta_que_falhou(client, job_queue):
    request = _request()
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")
    job_queue.resolve(
        request.query_id, QueryResult.failed(request.query_id, error="query_timeout")
    )

    response = client.get(f"/v1/query/{request.query_id}")

    assert response.status_code == 200
    assert response.json() == {
        "query_id": request.query_id,
        "status": "failed",
        "error": "query_timeout",
    }


def test_query_id_desconhecido_devolve_404(client):
    response = client.get("/v1/query/q_000000")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"] == "unknown_query"
    assert body["status"] == 404
