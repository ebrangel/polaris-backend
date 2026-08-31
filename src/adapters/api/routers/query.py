"""`POST /v1/query`, `GET /v1/query` (seção 2.2a) e `GET /v1/query/{query_id}`.

As três formas de submeter uma consulta — corpo JSON do POST, `query=<json>` e
parâmetros planos do GET — convergem para o mesmo `QueryRequestModel` e daí para o
mesmo `QueryRequest` de domínio **antes** de chegar no use case. Nenhuma regra de
negócio vive aqui: o router traduz HTTP, o `ExecuteQuery` decide.
"""

import json

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import ValidationError

from adapters.api.content_negotiation import CSV_MEDIA_TYPE, OutputFormat
from adapters.api.csv_presenter import csv_filename, csv_headers, csv_lines
from adapters.api.dependencies import (
    CatalogDep,
    ClientIdDep,
    ExecuteQueryDep,
    JobQueueDep,
    OutputFormatDep,
    ResultExporterDep,
    RolesDep,
)
from adapters.api.errors import (
    EXPORT_NOT_FOUND_TYPE,
    MALFORMED_REQUEST_TYPE,
    UNKNOWN_QUERY_TYPE,
    problem_response,
)
from adapters.api.query_params import parse_flat_params, schema_name_from_params
from adapters.api.schemas import QueryRequestModel, present_result
from application.ports.result_exporter import ExportMetadata, ResultExporter
from domain.models import QueryRequest, QueryResult, QueryStatus

router = APIRouter(prefix="/v1/query", tags=["query"])


def _malformed(detail: str, fields: list[str] | None = None) -> JSONResponse:
    return problem_response(
        type_=MALFORMED_REQUEST_TYPE,
        title="Requisição malformada",
        status=422,
        detail=detail,
        fields=fields,
    )


def _model_from_json(raw: str) -> QueryRequestModel | JSONResponse:
    """Opção A da seção 2.2a: `query` é o mesmo JSON do corpo do POST, url-encoded.

    A validação é feita à mão (e não pelo FastAPI) porque o JSON chega como texto de
    querystring — daí `ValidationError` precisar ser convertida aqui, em vez de cair no
    handler global.
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        return _malformed(f"`query` não é JSON válido: {exc.msg}.", ["query"])
    if not isinstance(payload, dict):
        return _malformed("`query` precisa ser um objeto JSON.", ["query"])
    try:
        return QueryRequestModel.model_validate(payload)
    except ValidationError as exc:
        fields = [
            ".".join(str(part) for part in error["loc"]) for error in exc.errors()
        ]
        return _malformed(
            "; ".join(f"{e['loc'][-1] if e['loc'] else 'query'}: {e['msg']}" for e in exc.errors()),
            fields or ["query"],
        )


def _export_not_found(query_id: str) -> JSONResponse:
    return problem_response(
        type_=EXPORT_NOT_FOUND_TYPE,
        title="Export não disponível",
        status=404,
        detail=(
            f"Não há arquivo para download da consulta '{query_id}' — ela não passou "
            "pela fila, o arquivo expirou, ou o servidor não tem export configurado."
        ),
        fields=[query_id],
    )


async def _file_response(
    exporter: ResultExporter,
    export: ExportMetadata,
    *,
    headers: dict[str, str] | None = None,
) -> Response:
    """Serve o arquivo que o worker gravou (seção 2.4a), em blocos.

    É o caminho que evita materializar o resultado no processo da API: os bytes vão do
    disco para o socket sem passar por um `QueryResult` em memória.
    """
    try:
        stream = await exporter.open(export.query_id)
    except FileNotFoundError:
        # Corrida contra a varredura de expirados, entre o `stat()` e o `open()`.
        return _export_not_found(export.query_id)

    return StreamingResponse(
        stream,
        status_code=200,
        media_type=CSV_MEDIA_TYPE,
        headers={
            "Content-Disposition": f'attachment; filename="{csv_filename(export.query_id)}"',
            "X-Query-Id": export.query_id,
            **(headers or {}),
            "Content-Length": str(export.size_bytes),
        },
    )


async def _completed_response(
    result: QueryResult,
    output_format: OutputFormat,
    *,
    exporter: ResultExporter | None = None,
    export: ExportMetadata | None = None,
) -> Response:
    """Corpo da seção 2.3 no formato negociado (seção 2.3a) — o **único** ponto do
    caminho da consulta em que o formato de saída importa.

    Havendo arquivo gravado pelo worker, o CSV sai dele em vez de ser renderizado de
    novo: mesmos bytes, sem o custo de reserializar, e o caminho vale para resultado de
    qualquer tamanho. Sem arquivo — toda consulta síncrona, que nunca passou pela fila —
    renderiza do resultado em memória, como antes.
    """
    if output_format is not OutputFormat.CSV:
        return JSONResponse(
            status_code=200, content=present_result(result, export=export)
        )

    if exporter is not None and export is not None:
        return await _file_response(exporter, export, headers=csv_headers(result))

    return StreamingResponse(
        csv_lines(result),
        status_code=200,
        media_type=CSV_MEDIA_TYPE,
        headers=csv_headers(result),
    )


async def _response_for(result: QueryResult, output_format: OutputFormat) -> Response:
    """202 para consulta enfileirada (seção 2.4); 200 para resultado pronto (2.3).

    A resposta de enfileiramento (e a de falha) sai **sempre em JSON**, qualquer que
    seja o formato pedido: `{query_id, status, poll_url}` não é uma tabela e não tem
    representação em CSV. Recusar CSV para consulta pesada seria pior — export grande é
    justamente o caso de uso do formato; o cliente enfileira, acompanha o status em
    JSON e baixa o CSV em `GET /v1/query/{query_id}?format=csv`.

    Não consulta o exportador: um resultado que chega por aqui foi executado agora ou
    veio do cache, nunca do worker — export existe só para consulta que passou pela
    fila, e é em `GET /v1/query/{query_id}` que ele aparece.
    """
    if result.status is QueryStatus.PROCESSING:
        return JSONResponse(status_code=202, content=present_result(result))
    if result.status is QueryStatus.FAILED:
        return JSONResponse(status_code=200, content=present_result(result))
    return await _completed_response(result, output_format)


async def _run(
    domain_request: QueryRequest,
    execute_query: ExecuteQueryDep,
    roles: RolesDep,
    client_id: ClientIdDep,
    output_format: OutputFormat,
) -> Response:
    result = await execute_query(domain_request, roles=roles, client_id=client_id)
    return await _response_for(result, output_format)


@router.post("")
async def post_query(
    body: QueryRequestModel,
    execute_query: ExecuteQueryDep,
    roles: RolesDep,
    client_id: ClientIdDep,
    output_format: OutputFormatDep,
) -> Response:
    """O formato de saída vem de `?format=`/`Accept`, e não do corpo: o corpo continua
    sendo exatamente `QueryRequestModel` (com `extra="forbid"`), o que garante que
    formato nenhum vaze para dentro do `QueryRequest` e do `query_id`."""
    return await _run(body.to_domain(), execute_query, roles, client_id, output_format)


@router.get("")
async def get_query(
    request: Request,
    catalog: CatalogDep,
    execute_query: ExecuteQueryDep,
    roles: RolesDep,
    client_id: ClientIdDep,
    output_format: OutputFormatDep,
) -> Response:
    """Recebe o `Request` cru: `filter[campo][operador]` usa chaves dinâmicas, que o
    FastAPI não consegue declarar como parâmetros (limitação anotada na seção 2.2a).

    `format` escapa da regra "se `query` estiver presente, os demais parâmetros são
    ignorados": aquela regra existe para não haver duas fontes de verdade da *consulta*,
    e o formato de saída não é parte da consulta — é transporte, lido pela dependência
    antes desta função, nas duas opções.
    """
    params = request.query_params

    raw_query = params.get("query")
    if raw_query is not None:
        # "Se `query` estiver presente, os demais parâmetros são ignorados."
        model = _model_from_json(raw_query)
        if isinstance(model, JSONResponse):
            return model
    else:
        schema = catalog.get_schema(schema_name_from_params(params))
        model = parse_flat_params(params, schema)

    return await _run(model.to_domain(), execute_query, roles, client_id, output_format)


@router.get("/{query_id}/download")
async def download_query_export(
    query_id: str, result_exporter: ResultExporterDep
) -> Response:
    """Baixa o CSV que o worker gravou para uma consulta pesada (seção 2.4a).

    Não toca na fila nem no cache: o arquivo é a fonte, e ele sobrevive ao TTL da
    entrada do job no Redis. Por isso os headers de `meta` (`X-Dataset-Used`,
    `X-Execution-Ms`) não aparecem aqui — quem quiser esses números pede
    `GET /v1/query/{query_id}`, enquanto o job existir.
    """
    export = None if result_exporter is None else await result_exporter.stat(query_id)
    if result_exporter is None or export is None:
        return _export_not_found(query_id)
    return await _file_response(result_exporter, export)


@router.get("/{query_id}")
async def get_query_status(
    query_id: str,
    job_queue: JobQueueDep,
    output_format: OutputFormatDep,
    result_exporter: ResultExporterDep,
) -> Response:
    """Status/resultado de uma consulta assíncrona (seção 2.4).

    Sempre 200 quando o `query_id` existe — inclusive com `status: processing`: aqui a
    leitura de status é que teve sucesso. O 202 é da submissão, não da consulta de
    status.

    Concluída, a resposta JSON ganha `download_url`/`download_expires_at` quando o
    worker deixou um arquivo para trás; com `?format=csv`, o próprio arquivo é servido.
    """
    result = await job_queue.get_status(query_id)
    if result is None:
        return problem_response(
            type_=UNKNOWN_QUERY_TYPE,
            title="Consulta não encontrada",
            status=404,
            detail=f"Nenhuma consulta com o identificador '{query_id}'.",
            fields=[query_id],
        )
    if result.status is not QueryStatus.COMPLETED:
        return JSONResponse(status_code=200, content=present_result(result))

    export = None if result_exporter is None else await result_exporter.stat(query_id)
    return await _completed_response(
        result, output_format, exporter=result_exporter, export=export
    )
