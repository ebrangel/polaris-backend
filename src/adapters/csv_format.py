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
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from adapters.serialization import jsonable
from domain.models import Column, QueryResult

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


class CsvRowFormatter:
    """Formatador incremental: recebe uma linha por vez, devolve o texto de uma linha.

    Existe para o caminho de streaming, onde as linhas chegam em blocos vindos do cursor
    e não há um `QueryResult` de onde iterar. Guarda o `csv.writer` e o `_LineSink` entre
    as chamadas, de modo que a configuração da RFC 4180 seja feita uma vez só e nenhum
    buffer cresça com o resultado: o custo de memória é sempre o de uma linha.
    """

    __slots__ = ("_sink", "_writer")

    def __init__(self, *, delimiter: str = DEFAULT_DELIMITER) -> None:
        self._sink = _LineSink()
        self._writer = csv.writer(
            self._sink,
            delimiter=delimiter,
            lineterminator=LINE_TERMINATOR,
            quoting=csv.QUOTE_MINIMAL,
        )

    def header(self, columns: Iterable[Column]) -> str:
        """Linha de cabeçalho com os nomes lógicos dos campos.

        Traz `Column.field` — o nome do modelo lógico que o cliente pediu, nunca a coluna
        física do dataset (convenção do CLAUDE.md).
        """
        self._writer.writerow([column.field for column in columns])
        return self._sink.take()

    def row(self, values: Sequence[Any]) -> str:
        self._writer.writerow([_cell(value) for value in values])
        return self._sink.take()


def csv_lines(
    result: QueryResult, *, delimiter: str = DEFAULT_DELIMITER
) -> Iterator[str]:
    """Linhas do CSV de um resultado já em memória — cabeçalho e, depois, os dados.

    É o caminho de quem tem as linhas em mãos: um acerto de cache respondido em CSV. Um
    gerador, então a API escreve linha a linha no socket sem montar o CSV inteiro como
    uma string única. Quem está lendo um cursor não passa por aqui — usa
    `CsvRowFormatter` direto, que é o que não exige um `QueryResult` materializado.
    """
    if result.rows is None:
        raise ValueError(
            f"O resultado '{result.query_id}' não carrega as linhas em memória — "
            "use `CsvRowFormatter` sobre a fonte que as tem."
        )

    formatter = CsvRowFormatter(delimiter=delimiter)
    yield formatter.header(result.columns)
    for row in result.rows:
        yield formatter.row(row)
