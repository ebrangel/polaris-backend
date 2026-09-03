"""Corpo JSON da seção 2.3 transmitido a partir do `.jsonl` do worker.

O caminho que antes não existia: um resultado que não cabe no cache não tinha de onde ser
respondido em JSON. Agora o descritor vem da fila (`rows=None`) e as linhas saem do
arquivo, costuradas dentro do envelope.
"""

import json

from application.ports.row_sink import StreamedResult
from domain.models import Column, DataType, QueryRequest, QueryResult

PAYLOAD = {
    "schema": "vendas",
    "dimensions": ["sigla_uf"],
    "measures": ["valor_total"],
}

COLUNAS = (
    Column(field="sigla_uf", type=DataType.STRING),
    Column(field="valor_total", type=DataType.NUMBER, format="currency"),
)


def _request() -> QueryRequest:
    return QueryRequest(
        schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",), limit=50_000
    )


async def _worker_gravou(exporter, query_id: str, linhas, *, total_rows=None):
    """Encena o worker: escreve os artefatos e devolve o descritor que volta pela fila."""
    sink = await exporter.open_writer(query_id, COLUNAS, "vendas_agregado_uf")
    await sink.write(list(linhas))
    streamed = StreamedResult(
        row_count=len(linhas),
        total_rows=total_rows if total_rows is not None else len(linhas),
        execution_ms=12,
    )
    await sink.close(streamed)
    return QueryResult.streamed(
        query_id=query_id,
        columns=COLUNAS,
        row_count=streamed.row_count,
        total_rows=streamed.total_rows,
        dataset_used="vendas_agregado_uf",
        execution_ms=12,
    )


async def test_post_devolve_as_linhas_vindas_do_arquivo(
    client_com_export, job_queue, exporter, financeiro
):
    """O worker concluiu dentro da janela inline e devolveu só o descritor. Sem consultar
    o arquivo, a resposta do `POST` sairia sem linhas."""
    request = _request()
    job_queue.default_result = await _worker_gravou(
        exporter, request.query_id, [("SP", 458320.50), ("RJ", 212904.10)], total_rows=27
    )

    response = client_com_export.post("/v1/query", json=PAYLOAD, headers=financeiro)

    assert response.status_code == 200
    body = response.json()
    assert body["rows"] == [["SP", 458320.50], ["RJ", 212904.10]]
    assert body["columns"] == [
        {"field": "sigla_uf", "type": "string"},
        {"field": "valor_total", "type": "number", "format": "currency"},
    ]
    assert body["meta"]["row_count"] == 2
    assert body["meta"]["total_rows"] == 27
    assert body["download_url"] == f"/v1/query/{request.query_id}/download"


async def test_corpo_transmitido_e_json_valido_com_muitas_linhas(
    client_com_export, job_queue, exporter, financeiro
):
    """O costurar-vírgulas entre blocos é onde um JSON transmitido quebra. Mil linhas
    atravessam vários blocos de leitura do arquivo."""
    request = _request()
    linhas = [(f"UF{i:04d}", float(i)) for i in range(1000)]
    job_queue.default_result = await _worker_gravou(exporter, request.query_id, linhas)

    response = client_com_export.post("/v1/query", json=PAYLOAD, headers=financeiro)

    body = json.loads(response.content)  # parse estrito: vírgula a mais/menos quebra aqui
    assert len(body["rows"]) == 1000
    assert body["rows"][0] == ["UF0000", 0.0]
    assert body["rows"][-1] == ["UF0999", 999.0]
    assert body["meta"]["row_count"] == 1000


async def test_resultado_vazio_transmitido_nao_gera_json_quebrado(
    client_com_export, job_queue, exporter, financeiro
):
    """Zero linhas é o caso em que a vírgula sobra: `rows:[` seguido direto de `],`."""
    request = _request()
    job_queue.default_result = await _worker_gravou(exporter, request.query_id, [])

    response = client_com_export.post("/v1/query", json=PAYLOAD, headers=financeiro)

    body = json.loads(response.content)
    assert body["rows"] == []
    assert body["meta"]["row_count"] == 0


async def test_status_por_id_tambem_transmite_do_arquivo(
    client_com_export, job_queue, exporter, financeiro
):
    request = _request()
    result = await _worker_gravou(
        exporter, request.query_id, [("SP", 1.0)], total_rows=99
    )
    await job_queue.enqueue(request, dataset_name="vendas_agregado_uf")
    job_queue.resolve(request.query_id, result)

    response = client_com_export.get(f"/v1/query/{request.query_id}")

    body = response.json()
    assert body["rows"] == [["SP", 1.0]]
    assert body["meta"]["total_rows"] == 99


async def test_sem_arquivo_e_sem_linhas_o_corpo_sai_honesto(
    client, job_queue, financeiro
):
    """Servidor sem export configurado: o descritor é verdadeiro, `rows` vem vazio e o
    `meta` diz quantas linhas existiam. Melhor que inventar linhas ou falhar."""
    request = _request()
    job_queue.default_result = QueryResult.streamed(
        query_id=request.query_id,
        columns=COLUNAS,
        row_count=1200,
        total_rows=1200,
        dataset_used="vendas_agregado_uf",
        execution_ms=12,
    )

    response = client.post("/v1/query", json=PAYLOAD, headers=financeiro)

    body = response.json()
    assert body["rows"] == []
    assert body["meta"]["row_count"] == 1200


async def test_status_responde_pelo_meta_depois_de_o_job_sumir_da_fila(
    client_com_export, exporter
):
    """O `arq` retém o resultado por `keep_result` (1h); o export dura bem mais. Antes,
    a consulta virava 404 enquanto o arquivo ainda estava baixável na rota ao lado."""
    request = _request()
    await _worker_gravou(exporter, request.query_id, [("SP", 1.0)], total_rows=42)
    # A fila nunca soube deste job — é o que acontece depois do `keep_result`.

    response = client_com_export.get(f"/v1/query/{request.query_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["meta"]["total_rows"] == 42
    assert body["rows"] == [["SP", 1.0]]
