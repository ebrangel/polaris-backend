"""Download do CSV gravado pelo worker (seção 2.4a).

O fluxo que estes testes cobrem é o de um export grande: a consulta pesada vai para a
fila, o worker executa e grava o arquivo, e o cliente descobre o `download_url` ao
acompanhar o status.
"""

import pytest

from domain.models import Column, DataType, QueryRequest, QueryResult

CSV_ESPERADO = (
    "sigla_uf,valor_total,quantidade\r\nSP,458320.5,1204\r\nRJ,212904.1,588\r\n"
)


def _request() -> QueryRequest:
    return QueryRequest(
        schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",)
    )


def _resultado(query_id: str) -> QueryResult:
    return QueryResult.completed(
        query_id=query_id,
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER, format="currency"),
            Column(field="quantidade", type=DataType.NUMBER),
        ),
        rows=(("SP", 458320.50, 1204), ("RJ", 212904.10, 588)),
        dataset_used="vendas_agregado_uf",
        execution_ms=1200,
    )


@pytest.fixture
async def concluida(job_queue, exporter):
    """Simula o worker: executa o job e grava o export, como `RunQueuedQuery` faz."""
    request = _request()
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")
    result = _resultado(request.query_id)
    job_queue.resolve(request.query_id, result)
    await exporter.export(result)
    return request.query_id


# --- descoberta do link -------------------------------------------------------------------


async def test_status_concluido_traz_download_url(client_com_export, concluida):
    body = client_com_export.get(f"/v1/query/{concluida}").json()

    assert body["status"] == "completed"
    assert body["download_url"] == f"/v1/query/{concluida}/download"
    assert body["download_expires_at"].startswith("20")


async def test_status_em_processamento_nao_traz_download_url(
    client_com_export, job_queue
):
    request = _request()
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")

    body = client_com_export.get(f"/v1/query/{request.query_id}").json()

    assert body["status"] == "processing"
    assert "download_url" not in body


async def test_resultado_inline_da_submissao_nao_traz_download_url(
    client_com_export, job_queue, financeiro
):
    """`download_url` só aparece em `GET /v1/query/{query_id}` — a resposta da submissão
    (mesmo concluída dentro da janela inline) não consulta o exportador."""
    job_queue.default_result = _resultado("q_8f2a1c")

    body = client_com_export.post(
        "/v1/query",
        json={"schema": "vendas", "dimensions": ["sigla_uf"], "measures": ["valor_total"]},
        headers=financeiro,
    ).json()

    assert body["status"] == "completed"
    assert "download_url" not in body


async def test_sem_exportador_configurado_nao_traz_download_url(client, job_queue):
    request = _request()
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")
    job_queue.resolve(request.query_id, _resultado(request.query_id))

    body = client.get(f"/v1/query/{request.query_id}").json()

    assert body["status"] == "completed"
    assert "download_url" not in body


# --- a rota de download -------------------------------------------------------------------


async def test_download_serve_o_arquivo(client_com_export, concluida):
    response = client_com_export.get(f"/v1/query/{concluida}/download")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8; header=present"
    assert response.headers["content-disposition"] == (
        f'attachment; filename="{concluida}.csv"'
    )
    assert response.headers["content-length"] == str(len(CSV_ESPERADO.encode("utf-8")))
    assert response.headers["x-query-id"] == concluida
    assert response.text == CSV_ESPERADO


async def test_download_nao_depende_da_fila(client_com_export, concluida, job_queue):
    """O arquivo é a fonte, e sobrevive ao TTL da entrada do job no Redis."""
    job_queue._jobs.clear()

    response = client_com_export.get(f"/v1/query/{concluida}/download")

    assert response.status_code == 200
    assert response.text == CSV_ESPERADO


async def test_download_de_query_id_desconhecido(client_com_export):
    response = client_com_export.get("/v1/query/q_000000/download")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"] == "export_not_found"
    assert body["fields"] == ["q_000000"]


async def test_download_de_export_expirado(client_com_export, concluida, exporter):
    exporter.expire(concluida)

    response = client_com_export.get(f"/v1/query/{concluida}/download")

    assert response.status_code == 404
    assert response.json()["type"] == "export_not_found"


async def test_download_sem_exportador_configurado(client, job_queue):
    request = _request()
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")
    job_queue.resolve(request.query_id, _resultado(request.query_id))

    response = client.get(f"/v1/query/{request.query_id}/download")

    assert response.status_code == 404
    assert response.json()["type"] == "export_not_found"


async def test_arquivo_removido_entre_o_stat_e_a_leitura(
    client_com_export, concluida, exporter
):
    """Corrida contra a varredura de expirados — vira 404, nunca resposta truncada."""
    exporter.vanished.add(concluida)

    response = client_com_export.get(f"/v1/query/{concluida}/download")

    assert response.status_code == 404
    assert response.json()["type"] == "export_not_found"


# --- `?format=csv` na rota de status ------------------------------------------------------


async def test_format_csv_serve_o_arquivo_quando_existe(client_com_export, concluida):
    """Mesmos bytes do download, mais os headers de `meta` que o status conhece."""
    response = client_com_export.get(f"/v1/query/{concluida}?format=csv")

    assert response.status_code == 200
    assert response.text == CSV_ESPERADO
    assert response.headers["content-length"] == str(len(CSV_ESPERADO.encode("utf-8")))
    assert response.headers["x-dataset-used"] == "vendas_agregado_uf"
    assert response.headers["x-execution-ms"] == "1200"


async def test_format_csv_sem_arquivo_renderiza_da_memoria(client, job_queue):
    """Sem exportador, o comportamento da seção 2.3a continua valendo."""
    request = _request()
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")
    job_queue.resolve(request.query_id, _resultado(request.query_id))

    response = client.get(f"/v1/query/{request.query_id}?format=csv")

    assert response.status_code == 200
    assert response.text == CSV_ESPERADO
    assert "content-length" not in response.headers or response.headers[
        "content-length"
    ] == str(len(CSV_ESPERADO.encode("utf-8")))
