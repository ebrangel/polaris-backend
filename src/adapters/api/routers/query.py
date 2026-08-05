"""`POST /v1/query`, `GET /v1/query` (seção 2.2a) e `GET /v1/query/{query_id}`.

As três formas de submeter uma consulta — corpo JSON do POST, `query=<json>` e
parâmetros planos do GET — convergem para o mesmo `QueryRequestModel` e daí para o
mesmo `QueryRequest` de domínio **antes** de chegar no use case. Nenhuma regra de
negócio vive aqui: o router traduz HTTP, o `ExecuteQuery` decide.
"""

import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from adapters.api.dependencies import CatalogDep, ExecuteQueryDep, JobQueueDep, RolesDep
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


def _response_for(result: QueryResult) -> JSONResponse:
    """202 para consulta enfileirada (seção 2.4); 200 para resultado pronto (2.3)."""
    status_code = 202 if result.status is QueryStatus.PROCESSING else 200
    return JSONResponse(status_code=status_code, content=present_result(result))


async def _run(
    domain_request: QueryRequest, execute_query: ExecuteQueryDep, roles: RolesDep
) -> JSONResponse:
    result = await execute_query(domain_request, roles=roles)
    return _response_for(result)


@router.post("")
async def post_query(
    body: QueryRequestModel, execute_query: ExecuteQueryDep, roles: RolesDep
) -> JSONResponse:
    return await _run(body.to_domain(), execute_query, roles)


@router.get("")
async def get_query(
    request: Request,
    catalog: CatalogDep,
    execute_query: ExecuteQueryDep,
    roles: RolesDep,
) -> JSONResponse:
    """Recebe o `Request` cru: `filter[campo][operador]` usa chaves dinâmicas, que o
    FastAPI não consegue declarar como parâmetros (limitação anotada na seção 2.2a)."""
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

    return await _run(model.to_domain(), execute_query, roles)


@router.get("/{query_id}")
async def get_query_status(query_id: str, job_queue: JobQueueDep) -> JSONResponse:
    """Status/resultado de uma consulta assíncrona (seção 2.4).

    Sempre 200 quando o `query_id` existe — inclusive com `status: processing`: aqui a
    leitura de status é que teve sucesso. O 202 é da submissão, não da consulta de
    status.
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
    return JSONResponse(status_code=200, content=present_result(result))
