"""Tradução de erro de domínio → resposta HTTP no formato `application/problem+json`
da seção 2.5.

O mapa `type → status` vive aqui, não no domínio: código HTTP é detalhe de transporte
(decisão registrada no Marco 2, quando `DomainError` foi criado sem campo `status`).

A documentação só fixa dois códigos — `422` para `no_dataset_available` (exemplo da
seção 2.5) e `429` para `rate_limited` (`docs/escalabilidade.md`, "fila cheia →
backpressure: 429"). Os demais são escolha deste adapter, anotada abaixo.
"""

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from adapters.api.content_negotiation import UnsupportedFormatError
from domain.errors import (
    DomainError,
    ForbiddenMeasureError,
    InvalidCatalogError,
    InvalidFilterError,
    NoDatasetAvailableError,
    QueryTimeoutError,
    RateLimitedError,
    UnknownFieldError,
    UnknownSchemaError,
)

PROBLEM_JSON = "application/problem+json"

#: `type` do envelope → código HTTP. Só 422/no_dataset_available e 429/rate_limited vêm
#: da documentação; o resto segue a semântica HTTP usual.
_STATUS_BY_ERROR: dict[type[DomainError], int] = {
    UnknownSchemaError: 404,  # o schema é o recurso da URL/corpo — não existe
    UnknownFieldError: 422,  # sintaxe válida, semântica inválida contra o modelo lógico
    InvalidFilterError: 422,
    ForbiddenMeasureError: 403,  # autenticado, mas o role não alcança a medida
    NoDatasetAvailableError: 422,  # documentado na seção 2.5
    QueryTimeoutError: 504,  # a API é o gateway; quem estourou o prazo foi o datasource
    RateLimitedError: 429,  # documentado em docs/escalabilidade.md
    InvalidCatalogError: 422,  # catálogo bem formado, mas semanticamente inválido (Marco 8)
}

#: Erros de validação de forma (Pydantic/FastAPI) não são `DomainError`: o corpo nem
#: chegou a virar um `QueryRequest`. Entram no mesmo envelope com um `type` próprio.
MALFORMED_REQUEST_TYPE = "malformed_request"

#: `query_id` inexistente em `GET /v1/query/{query_id}`. A lista de `type` da seção 2.5
#: não cobre este caso — acrescentado aqui, como o `invalid_catalog` do Marco 1.
UNKNOWN_QUERY_TYPE = "unknown_query"

#: `type` genérico para as poucas rotas (administrativas, Marco 8) que ainda levantam
#: `HTTPException` do FastAPI em vez de um `DomainError` — token interno ausente, etc.
HTTP_ERROR_TYPE = "http_error"

#: `?format=` com valor que a API não produz (seção 2.3a). Como `unknown_query`, é um
#: `type` que a lista da seção 2.5 não previa.
INVALID_FORMAT_TYPE = "invalid_format"

#: Download de um export que não existe, expirou, ou nunca foi gerado porque o servidor
#: não tem exportador configurado (seção 2.4a). Os três casos respondem igual de
#: propósito: para o cliente, "não há arquivo para baixar" é uma informação só.
EXPORT_NOT_FOUND_TYPE = "export_not_found"


def status_for(error: DomainError) -> int:
    return _STATUS_BY_ERROR.get(type(error), 500)


def problem_response(
    *, type_: str, title: str, status: int, detail: str, fields: list[str] | None = None
) -> JSONResponse:
    body: dict[str, object] = {
        "type": type_,
        "title": title,
        "status": status,
        "detail": detail,
    }
    if fields:
        body["fields"] = fields
    return JSONResponse(status_code=status, content=body, media_type=PROBLEM_JSON)


async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
    """Envelope da seção 2.5 a partir do que o próprio erro de domínio já carrega."""
    status = status_for(exc)
    problem = exc.as_problem()
    return problem_response(
        type_=str(problem["type"]),
        title=str(problem["title"]),
        status=status,
        detail=str(problem["detail"]),
        fields=list(exc.fields) if exc.fields else None,
    )


async def validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Erro de forma (JSON malformado, operador fora do enum, tipo errado).

    Sem isso o FastAPI responderia no formato padrão dele (`{"detail": [...]}`), que
    não é o envelope único que a seção 2.5 exige.
    """
    fields = []
    for error in exc.errors():
        location = [str(part) for part in error["loc"] if part not in ("body", "query")]
        if location:
            fields.append(".".join(location))

    return problem_response(
        type_=MALFORMED_REQUEST_TYPE,
        title="Requisição malformada",
        status=422,
        detail="; ".join(f"{e['loc'][-1]}: {e['msg']}" for e in exc.errors()),
        fields=fields or None,
    )


async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    """Mesmo envelope da seção 2.5 para as rotas administrativas, que levantam
    `HTTPException` (ex: token interno inválido) em vez de um `DomainError`."""
    return problem_response(
        type_=HTTP_ERROR_TYPE,
        title="Erro na requisição",
        status=exc.status_code,
        detail=str(exc.detail),
    )


async def unsupported_format_handler(
    request: Request, exc: UnsupportedFormatError
) -> JSONResponse:
    """`?format=` desconhecido (seção 2.3a).

    Erro de formato de saída é respondido **em JSON**, como todos os outros: um corpo
    de erro em CSV não teria onde carregar `type`/`title`/`detail`, e o cliente que
    pediu CSV precisa justamente da explicação do que deu errado.
    """
    return problem_response(
        type_=INVALID_FORMAT_TYPE,
        title="Formato de saída não suportado",
        status=422,
        detail=(
            f"Formato de saída '{exc.requested}' não existe — "
            f"use um de: {', '.join(exc.supported)}."
        ),
        fields=["format"],
    )
