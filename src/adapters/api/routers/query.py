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
from adapters.api.csv_presenter import csv_headers, csv_lines
from adapters.api.dependencies import (
    CatalogDep,
    ClientIdDep,
    ExecuteQueryDep,
    JobQueueDep,
    OutputFormatDep,
    RolesDep,
)
from adapters.api.errors import (
    MALFORMED_REQUEST_TYPE,
    UNKNOWN_QUERY_TYPE,
    problem_response,
)
from adapters.api.query_params import parse_flat_params, schema_name_from_params
from adapters.api.schemas import QueryRequestModel, present_result
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


def _completed_response(result: QueryResult, output_format: OutputFormat) -> Response:
    """Corpo da seção 2.3 no formato negociado (seção 2.3a) — o **único** ponto do
    caminho da consulta em que o formato de saída importa."""
    if output_format is OutputFormat.CSV:
        return StreamingResponse(
            csv_lines(result),
            status_code=200,
            media_type=CSV_MEDIA_TYPE,
            headers=csv_headers(result),
        )
    return JSONResponse(status_code=200, content=present_result(result))


def _response_for(result: QueryResult, output_format: OutputFormat) -> Response:
    """202 para consulta enfileirada (seção 2.4); 200 para resultado pronto (2.3).

    A resposta de enfileiramento (e a de falha) sai **sempre em JSON**, qualquer que
    seja o formato pedido: `{query_id, status, poll_url}` não é uma tabela e não tem
    representação em CSV. Recusar CSV para consulta pesada seria pior — export grande é
    justamente o caso de uso do formato; o cliente enfileira, acompanha o status em
    JSON e baixa o CSV em `GET /v1/query/{query_id}?format=csv`.
    """
    if result.status is QueryStatus.PROCESSING:
        return JSONResponse(status_code=202, content=present_result(result))
    if result.status is QueryStatus.FAILED:
        return JSONResponse(status_code=200, content=present_result(result))
    return _completed_response(result, output_format)


async def _run(
    domain_request: QueryRequest,
    execute_query: ExecuteQueryDep,
    roles: RolesDep,
    client_id: ClientIdDep,
    output_format: OutputFormat,
) -> Response:
    result = await execute_query(domain_request, roles=roles, client_id=client_id)
    return _response_for(result, output_format)


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


@router.get("/{query_id}")
async def get_query_status(
    query_id: str, job_queue: JobQueueDep, output_format: OutputFormatDep
) -> Response:
    """Status/resultado de uma consulta assíncrona (seção 2.4).

    Sempre 200 quando o `query_id` existe — inclusive com `status: processing`: aqui a
    leitura de status é que teve sucesso. O 202 é da submissão, não da consulta de
    status.

    Com `?format=csv` este é o passo de download do fluxo assíncrono: o cliente
    acompanha o status em JSON e, quando `completed`, baixa o resultado como arquivo.
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
    if result.status is QueryStatus.COMPLETED:
        return _completed_response(result, output_format)
    return JSONResponse(status_code=200, content=present_result(result))
