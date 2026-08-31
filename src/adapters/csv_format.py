"""`QueryResult` (domain) → linhas de CSV da RFC 4180, compartilhado entre a API e o
worker.

Mora aqui, e não em `adapters/api/`, pelo mesmo motivo de `adapters/serialization.py`:
dois adapters de processos diferentes precisam da mesma tradução. A API usa estas
linhas para responder `?format=csv` (`adapters/api/csv_presenter.py`); o worker usa
para gravar o arquivo de export (`adapters/exports/local_file_exporter.py`), e um
adapter de fila não pode depender de um adapter de HTTP.

**Valores crus, não formatados.** `Column.format` (`"currency"`) é dica de
apresentação para o cliente e é deliberadamente ignorada aqui: escrever
`R$ 458.320,50` inutilizaria o arquivo para qualquer consumo programático. Pela mesma
razão o `Decimal` sai como texto exato vindo de `jsonable()`, sem virar `float`.
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

    É um gerador, então quem consome escreve linha a linha (no socket, na API; no
    arquivo, no worker) sem montar o CSV inteiro como uma string única. O
    `QueryResult` em si já está materializado — reduzir *isso* é o que exigiria um port
    de execução por streaming, fora do escopo deste módulo.
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
