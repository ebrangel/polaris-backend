"""`QueryResult` (domain) → CSV da RFC 4180 — o segundo presenter da seção 2.3a.

Fica ao lado de `present_result()` (`schemas.py`, o presenter JSON) e é a única peça
que o CSV acrescenta ao caminho da consulta: `QueryResult` já é uma tabela
(`columns` + `rows`), então nada muda em `domain/`, `application/` nem nos executores.

Módulo puro (sem FastAPI), como `query_params.py` e `content_negotiation.py`.

**Valores crus, não formatados.** `Column.format` (`"currency"`) é dica de
apresentação para o cliente e é deliberadamente ignorada aqui: escrever
`R$ 458.320,50` inutilizaria o arquivo para qualquer consumo programático. Pela mesma
razão o `Decimal` sai como texto exato vindo de `jsonable()`, sem virar `float`.

**Não é streaming de verdade.** As linhas são geradas sob demanda, o que evita montar
o arquivo inteiro em memória além das linhas que já existem — mas o `QueryExecutor`
devolve um `QueryResult` com todas as linhas materializadas, e cache e fila serializam
o resultado inteiro. O `format: "csv_stream"` da seção 2.6 exige um port novo
(`execute_stream` devolvendo um `AsyncIterator` de linhas), com desvio de cache e de
fila; é marco próprio, não detalhe deste presenter.
"""

import csv
import json
from collections.abc import Iterator
from typing import Any

from adapters.serialization import jsonable
from domain.models import QueryResult

#: RFC 4180 §2.1: registros terminados por CRLF.
LINE_TERMINATOR = "\r\n"

#: RFC 4180 §2.4: vírgula. Excel em pt-BR espera `;` — daí o delimitador ser
#: parametrizável, mas o padrão continua sendo o da RFC, que é o que qualquer
#: consumidor programático assume.
DEFAULT_DELIMITER = ","


class _LineSink:
    """Alvo de escrita do `csv.writer`, que chama `write` uma vez por registro."""

    def __init__(self) -> None:
        self._line = ""

    def write(self, text: str) -> int:
        self._line = text
        return len(text)

    def take(self) -> str:
        line, self._line = self._line, ""
        return line


def _cell(value: Any) -> Any:
    """Valor da linha → o que o `csv.writer` escreve.

    Reaproveita o `jsonable()` do cache/fila (`Decimal` → texto sem perda de precisão,
    datas em ISO 8601, `UUID`/`bytes` em texto), com três ajustes próprios do CSV:

    - `None` vira campo **vazio** — nunca `"None"` nem `"null"` (o `csv.writer` já faz
      isso; a passagem explícita por aqui é para não deixar a regra implícita);
    - `bool` vira `true`/`false`, e não o `True`/`False` que sairia de `str()` — assim
      a mesma consulta em JSON e em CSV traz o mesmo texto;
    - lista (valor multivalorado vindo do Elasticsearch) vira JSON dentro da célula,
      que é o único jeito de não perder a estrutura numa grade bidimensional.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    normalized = jsonable(value)
    if isinstance(normalized, list):
        return json.dumps(normalized, ensure_ascii=False)
    return normalized


def csv_lines(
    result: QueryResult, *, delimiter: str = DEFAULT_DELIMITER
) -> Iterator[str]:
    """Linhas do CSV: cabeçalho com os nomes lógicos dos campos e, depois, os dados.

    O cabeçalho traz `Column.field` — o mesmo nome lógico do modelo lógico do schema
    que o cliente pediu, nunca a coluna física do dataset (convenção do CLAUDE.md).
    """
    sink = _LineSink()
    writer = csv.writer(
        sink,
        delimiter=delimiter,
        lineterminator=LINE_TERMINATOR,
        quoting=csv.QUOTE_MINIMAL,
    )

    writer.writerow([column.field for column in result.columns])
    yield sink.take()

    for row in result.rows:
        writer.writerow([_cell(value) for value in row])
        yield sink.take()


def csv_filename(result: QueryResult) -> str:
    """Nome do arquivo baixado. O `query_id` identifica a consulta e é estável entre
    requisições idênticas, o que dá nomes reproduzíveis."""
    return f"{result.query_id}.csv"


def csv_headers(result: QueryResult) -> dict[str, str]:
    """Headers da resposta CSV, incluindo o `meta` da seção 2.3.

    O CSV é uma grade de dados e não tem onde carregar `dataset_used`, `cached` ou
    `execution_ms` sem estragar o arquivo para quem o abre numa planilha — então esses
    metadados saem fora de banda, em headers. A observabilidade do Marco 9 continua
    disponível para quem consome a API por código.
    """
    headers = {
        "Content-Disposition": f'attachment; filename="{csv_filename(result)}"',
        "X-Query-Id": result.query_id,
    }
    if result.meta is not None:
        headers["X-Row-Count"] = str(result.meta.row_count)
        headers["X-Cached"] = "true" if result.meta.cached else "false"
        headers["X-Execution-Ms"] = str(result.meta.execution_ms)
        headers["X-Dataset-Used"] = result.meta.dataset_used
    return headers
