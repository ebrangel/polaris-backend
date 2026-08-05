"""Erros de domínio.

Cada erro corresponde a um dos `type` previstos na seção 2.5 de
`docs/catalogo-e-contrato-completo.md`, no formato `application/problem+json`.
O código HTTP **não** é definido aqui: mapear `type` para status é responsabilidade
do adapter de API (Marco 6), não do domínio.
"""

from collections.abc import Iterable
from typing import ClassVar

__all__ = [
    "DomainError",
    "ForbiddenMeasureError",
    "InvalidCatalogError",
    "InvalidFilterError",
    "NoDatasetAvailableError",
    "QueryTimeoutError",
    "RateLimitedError",
    "UnknownFieldError",
    "UnknownSchemaError",
]


class DomainError(Exception):
    """Base de todos os erros de domínio, com os campos do envelope de erro."""

    type: ClassVar[str] = "domain_error"
    title: ClassVar[str] = "Erro de domínio"

    def __init__(self, detail: str, fields: Iterable[str] = ()) -> None:
        super().__init__(detail)
        self.detail = detail
        self.fields: tuple[str, ...] = tuple(fields)

    def as_problem(self) -> dict[str, object]:
        """Envelope de erro sem o `status` — o adapter de API acrescenta o código HTTP."""
        problem: dict[str, object] = {
            "type": self.type,
            "title": self.title,
            "detail": self.detail,
        }
        if self.fields:
            problem["fields"] = list(self.fields)
        return problem


class UnknownSchemaError(DomainError):
    type = "unknown_schema"
    title = "Schema desconhecido"


class UnknownFieldError(DomainError):
    type = "unknown_field"
    title = "Campo desconhecido no modelo lógico"


class InvalidFilterError(DomainError):
    type = "invalid_filter"
    title = "Filtro inválido"


class ForbiddenMeasureError(DomainError):
    type = "forbidden_measure"
    title = "Medida não permitida para o role do usuário"


class NoDatasetAvailableError(DomainError):
    type = "no_dataset_available"
    title = "Nenhum dataset cobre os campos pedidos"

    @classmethod
    def for_request(
        cls,
        schema_name: str,
        fields: Iterable[str],
    ) -> "NoDatasetAvailableError":
        """Erro levantado quando nenhum dataset do schema cobre a combinação pedida.

        `fields` é a lista ordenada de campos referenciados pela requisição (saída,
        filtro e ordenação juntos) — a seção 2.5 não distingue dimensão de medida na
        mensagem: "...provê a combinação de campos: sigla_uf, cargo, canal."
        """
        fields = tuple(fields)
        return cls(
            f"Nenhum dataset do schema '{schema_name}' provê a combinação de campos: "
            f"{', '.join(fields)}.",
            fields,
        )


class QueryTimeoutError(DomainError):
    type = "query_timeout"
    title = "Tempo de execução da consulta excedido"


class RateLimitedError(DomainError):
    type = "rate_limited"
    title = "Limite de requisições excedido"


class InvalidCatalogError(DomainError):
    """Catálogo malformado.

    Não é um erro de requisição: é detectado ao construir as entidades a partir do
    YAML e reportado pelo pipeline de publicação (Marco 8).
    """

    type = "invalid_catalog"
    title = "Catálogo inválido"
