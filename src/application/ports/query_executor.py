"""Port de execução de consultas contra um dataset.

Uma implementação por família de datasource: `SQLAlchemyQueryExecutor` (Postgres,
Oracle) e `ElasticsearchQueryExecutor` (Marco 5) — ambas atendem este mesmo contrato,
para que o use case `RunQueuedQuery` (worker) não precise saber qual delas está usando.

**O executor não devolve linhas** (Marco 12): ele lê o cursor em blocos e empurra cada
bloco para um `RowSink`, devolvendo só os números do fim. Um port que devolvesse
`QueryResult` obrigaria a materializar o resultado inteiro em memória antes de qualquer
destino vê-lo — que é exatamente o que este marco eliminou. Ver `ports/row_sink.py` para
o porquê de o modelo ser push e não pull.
"""

from typing import Protocol, runtime_checkable

from application.ports.row_sink import RowSink, StreamedResult
from domain.models import Column, Dataset, QueryRequest


@runtime_checkable
class QueryExecutor(Protocol):
    """Executa uma `QueryRequest` já resolvida para um `Dataset` específico."""

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        sink: RowSink,
    ) -> StreamedResult:
        """Executa a consulta, transmite as linhas para `sink` e devolve seus números.

        `columns` vem de `Schema.columns_for(request)` — o executor usa os tipos e o
        `format` de cada coluna para montar a resposta da seção 2.3, sem precisar
        conhecer o `Schema` inteiro. É também o que define a largura das linhas
        entregues ao sink.

        O executor só chama `sink.write`. Abrir e fechar é de quem construiu o sink (o
        use case): ele tem as `columns` para o `open` e é o único que sabe se os demais
        destinos também terminaram bem. Levanta `domain.errors.QueryTimeoutError` se a
        consulta estourar o timeout configurado no executor — o timeout cobre a leitura
        inteira do cursor, não só o envio da consulta.
        """
        ...
