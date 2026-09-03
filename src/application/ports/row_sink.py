"""Port do destino das linhas de um resultado — o eixo da execução por streaming.

O executor **empurra** blocos de linhas para um `RowSink` em vez de devolver o resultado
pronto. A razão é o tempo de vida do cursor: um `AsyncResult` do SQLAlchemy só é válido
enquanto a conexão que o produziu está aberta, e a conexão fecha no fim do
`async with engine.connect()` dentro do executor. Um port que devolvesse um iterador
entregaria ao chamador um cursor já morto; com o modelo push, o laço de leitura e a
escrita nos destinos acontecem os dois dentro daquele `async with`.

**Onde cada implementação mora.** `RowSink` é declarado aqui, em `application/`, e as
implementações concretas ficam em `adapters/` — arquivo CSV, arquivo JSONL, Redis. É a
mesma razão de `ResultExporter` receber `QueryResult` e não linhas de CSV já formatadas
(ver o docstring daquele port): formato é assunto de adapter, e `application/` não pode
importar `adapters/`. O que atravessa esta fronteira são tuplas de valores de domínio.

**Contrato de ciclo de vida.** O sink já nasce aberto — quem o cria é uma fábrica que já
tem as colunas em mãos (`ResultExporter.open_writer`, `CacheGateway.open_writer`), então
não há um `open()` separado a esquecer. Daí em diante: `write` zero ou mais vezes, e
exatamente um entre `close` (sucesso) e `abort` (desistência ou falha). Depois de
`close`/`abort` o sink não aceita mais escrita. Implementações que gravam arquivo devem
tornar o resultado visível só no `close` — escrever num temporário e renomear —, para que
um leitor concorrente nunca encontre um artefato pela metade.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class StreamedResult:
    """O que se sabe do resultado depois de o cursor ter sido drenado.

    É o retorno de `QueryExecutor.execute` e a entrada de `RowSink.close`: os números que
    só existem no fim, e que antes vinham de `len(result.rows)`.
    """

    #: Linhas efetivamente lidas — o `meta.row_count` da seção 2.3.
    row_count: int
    #: Linhas que existiriam sem `limit`/`offset`, ou `None` quando não foi apurado.
    total_rows: int | None
    execution_ms: int


@runtime_checkable
class RowSink(Protocol):
    """Destino de um resultado, alimentado bloco a bloco enquanto o cursor é lido."""

    async def write(self, rows: Sequence[tuple[Any, ...]]) -> None:
        """Acrescenta um bloco de linhas, na ordem em que o banco as devolveu.

        Cada linha tem exatamente `len(columns)` valores — a coluna auxiliar da contagem
        total já foi retirada pelo executor e nunca chega aqui.
        """
        ...

    async def close(self, result: StreamedResult) -> None:
        """Finaliza o destino e o torna visível.

        Recebe os números do resultado porque vários destinos precisam deles no fim: o
        arquivo de metadados os grava, e o sink de cache decide com eles se o que
        acumulou vale a pena guardar.
        """
        ...

    async def abort(self) -> None:
        """Descarta o que foi escrito, sem tornar nada visível.

        Chamado quando a consulta falha no meio, ou quando o próprio sink desiste (um
        resultado grande demais para o cache, por exemplo). Precisa ser idempotente e não
        levantar: é executado em caminho de erro, onde uma segunda exceção só esconderia
        a primeira.
        """
        ...
