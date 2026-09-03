"""Modelos Pydantic da borda HTTP + tradução para/de entidades de domínio.

Entrada: JSON (seção 2.2) → `QueryRequestModel` → `QueryRequest` (domain). O `POST` e a
opção A do `GET` (seção 2.2a, `query=<json>`) usam **este mesmo modelo** — é o que o
documento chama de "validado pelo mesmo modelo Pydantic — zero lógica de parsing nova".

Saída: `QueryResult` (domain) → dicionários no formato das seções 2.3 e 2.4. As
respostas são montadas à mão, e não por `response_model`, porque o contrato omite
chaves ausentes (`format` só aparece na coluna que tem um) — controle explícito é mais
simples que configurar exclusão de nulos em modelos aninhados.
"""

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from adapters.serialization import jsonable
from application.ports.result_exporter import ExportMetadata
from domain.models import (
    Filter,
    FilterOperator,
    OrderBy,
    QueryRequest,
    QueryResult,
    QueryStatus,
    ResultMeta,
    Schema,
    SortDirection,
)


class FilterModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    operator: FilterOperator
    value: Any


class OrderByModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    direction: SortDirection = SortDirection.ASC


class QueryRequestModel(BaseModel):
    """Corpo da seção 2.2.

    O campo se chama `schema` no fio (é o contrato), mas o atributo Python é
    `schema_name`: `schema` sombrearia `BaseModel.schema()` e o Pydantic emite aviso.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    schema_name: str = Field(alias="schema")
    dimensions: list[str] = Field(default_factory=list)
    measures: list[str] = Field(default_factory=list)
    filters: list[FilterModel] = Field(default_factory=list)
    order_by: list[OrderByModel] = Field(default_factory=list)
    limit: int | None = None
    offset: int = 0

    def to_domain(self) -> QueryRequest:
        """Converte para a entidade de domínio — o ponto de convergência de POST e GET.

        As invariantes (operador `in` com lista vazia, `limit` negativo, ...) são
        verificadas pelo próprio domínio na construção e sobem como `DomainError`.
        """
        return QueryRequest(
            schema=self.schema_name,
            dimensions=tuple(self.dimensions),
            measures=tuple(self.measures),
            filters=tuple(
                Filter(field=f.field, operator=f.operator, value=f.value)
                for f in self.filters
            ),
            order_by=tuple(
                OrderBy(field=o.field, direction=o.direction) for o in self.order_by
            ),
            limit=self.limit,
            offset=self.offset,
        )


def poll_url_for(query_id: str) -> str:
    return f"/v1/query/{query_id}"


def download_url_for(query_id: str) -> str:
    return f"/v1/query/{query_id}/download"


def present_result(
    result: QueryResult, *, export: ExportMetadata | None = None
) -> dict[str, Any]:
    """`QueryResult` → corpo da seção 2.3 (concluído/falho) ou 2.4 (em processamento).

    `export` é o arquivo que o worker gravou para esta consulta, quando existe (seção
    2.4a): dele saem `download_url` e `download_expires_at`. Como o `poll_url`, os dois
    são **detalhe de transporte** montados aqui — o `QueryResult` do domínio não os
    carrega, e uma consulta síncrona (que nunca passou pelo worker) simplesmente não
    tem export, então as chaves não aparecem.
    """
    if result.status is QueryStatus.PROCESSING:
        return {
            "query_id": result.query_id,
            "status": result.status.value,
            "poll_url": poll_url_for(result.query_id),
        }

    if result.status is QueryStatus.FAILED:
        return {
            "query_id": result.query_id,
            "status": result.status.value,
            "error": result.error,
        }

    columns = []
    for column in result.columns:
        item: dict[str, Any] = {"field": column.field, "type": column.type.value}
        if column.format is not None:
            item["format"] = column.format
        columns.append(item)

    assert result.meta is not None  # garantido pelas invariantes de QueryResult
    body: dict[str, Any] = {
        "query_id": result.query_id,
        "status": result.status.value,
        "columns": columns,
        # Os routers devolvem `JSONResponse` já montada (para controlar 200 vs. 202 vs.
        # os códigos da seção 2.5), o que passa ao largo da serialização automática do
        # FastAPI — sem isto, uma coluna `numeric`/`date` do Postgres (→ `Decimal`/
        # `date`) derrubaria a resposta com `TypeError`.
        "rows": [[jsonable(value) for value in row] for row in result.rows or ()],
        "meta": present_meta(result.meta),
    }
    if export is not None:
        body["download_url"] = download_url_for(result.query_id)
        body["download_expires_at"] = export.expires_at.isoformat()
    return body


def present_meta(meta: ResultMeta) -> dict[str, Any]:
    """O bloco `meta` da seção 2.3, compartilhado com o corpo JSON transmitido.

    `total_rows` sai mesmo quando é `None`, e isso é deliberado: omitir a chave faria o
    cliente não saber distinguir "este servidor não informa o total" de "o total não foi
    apurado para esta consulta". Explícito, `null` diz a segunda coisa.
    """
    return {
        "row_count": meta.row_count,
        "cached": meta.cached,
        "execution_ms": meta.execution_ms,
        "dataset_used": meta.dataset_used,
        "total_rows": meta.total_rows,
    }


def json_envelope(
    result: QueryResult, *, export: ExportMetadata | None = None
) -> tuple[bytes, bytes]:
    """Prefixo e sufixo do corpo da seção 2.3, para as linhas serem costuradas no meio.

    É o que permite responder JSON a partir do arquivo `.jsonl` sem materializar as linhas:
    a API escreve o prefixo, repassa o arquivo (cujas linhas já são listas JSON, uma por
    linha, e só precisam de vírgulas entre elas) e fecha com o sufixo.

    Note que `meta` vai no **fim**, e não no começo: seus números vêm do descritor, mas
    montar o envelope inteiro antes de conhecer o corpo é o que deixa o prefixo ser
    escrito no socket imediatamente.
    """
    assert result.meta is not None
    columns = []
    for column in result.columns:
        item: dict[str, Any] = {"field": column.field, "type": column.type.value}
        if column.format is not None:
            item["format"] = column.format
        columns.append(item)

    head = (
        '{"query_id":' + json.dumps(result.query_id)
        + ',"status":' + json.dumps(result.status.value)
        + ',"columns":' + json.dumps(columns)
        + ',"rows":['
    )

    tail_body: dict[str, Any] = {"meta": present_meta(result.meta)}
    if export is not None:
        tail_body["download_url"] = download_url_for(result.query_id)
        tail_body["download_expires_at"] = export.expires_at.isoformat()
    # `json.dumps` de um dict e depois a troca da chave de abertura por uma vírgula: o
    # sufixo é a cauda de um objeto já aberto pelo prefixo.
    tail = "]," + json.dumps(tail_body)[1:]

    return head.encode("utf-8"), tail.encode("utf-8")


def present_schema_summary(schema: Schema) -> dict[str, Any]:
    """Item de `GET /v1/catalog` — só nome e descrição."""
    summary: dict[str, Any] = {"schema": schema.name, "version": schema.version}
    if schema.description is not None:
        summary["description"] = schema.description
    return summary


def present_schema_detail(schema: Schema) -> dict[str, Any]:
    """Corpo de `GET /v1/catalog/{schema}` — **só o modelo lógico**.

    "A resposta desse endpoint mostra apenas o modelo lógico (dimensões/medidas) — os
    datasets e seu roteamento são detalhe interno, não expostos ao cliente" (seção 2.1).
    A tabela de endpoints da mesma seção diz o contrário ("...e datasets"); seguimos a
    prosa, que é a que casa com a convenção do CLAUDE.md de que o cliente não conhece a
    estrutura física.
    """
    detail = present_schema_summary(schema)
    detail["dimensions"] = [
        {"name": dim.name, "type": dim.type.value, "filterable": dim.filterable}
        for dim in schema.dimensions.values()
    ]

    measures = []
    for measure in schema.measures.values():
        item: dict[str, Any] = {"name": measure.name, "agg": measure.agg.value}
        if measure.format is not None:
            item["format"] = measure.format
        measures.append(item)
    detail["measures"] = measures

    return detail
