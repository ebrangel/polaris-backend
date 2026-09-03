"""`ElasticsearchQueryExecutor` — implementa `QueryExecutor` (Marco 2), traduzindo a
requisição estruturada para agregações da Query DSL via `AsyncElasticsearch`.

Um só cliente (o `elasticsearch-py` já tem pool HTTP próprio — "Elasticsearch não usa
pool de conexões relacional", `docs/escalabilidade.md`) e um único timeout: todas as
consultas rodam pelo worker.

**Fora da leitura em blocos do Marco 12, de propósito.** Este adapter atende o port novo,
mas materializa a resposta como sempre fez e empurra tudo num único `write`: o
Elasticsearch devolve a agregação inteira num corpo HTTP, não há cursor a paginar, e o
`max_limit` de um índice no catálogo é da ordem de mil linhas — não é a fonte do estouro
de memória que motivou a mudança. Pela mesma razão `total_rows` vem `None`: não existe
função de janela, e o `hits.total` de uma busca com `size: 0` conta *documentos*, não
buckets — devolvê-lo seria informar um número que responde a outra pergunta.
"""

import asyncio

from elasticsearch import AsyncElasticsearch

from adapters.executors.elasticsearch_dsl import build_query_body, parse_response
from application.ports.row_sink import RowSink, StreamedResult
from domain.errors import QueryTimeoutError
from domain.models import Column, Dataset, IndexModel, QueryRequest


class ElasticsearchQueryExecutor:
    """Executa `QueryRequest` já resolvidas contra um índice único do Elasticsearch."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        timeout_seconds: float = 300.0,
    ) -> None:
        self._client = client
        self._timeout_seconds = timeout_seconds

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        sink: RowSink,
    ) -> StreamedResult:
        assert isinstance(dataset.model, IndexModel)
        body = build_query_body(dataset, request, columns)

        try:
            response = await asyncio.wait_for(
                self._client.search(index=dataset.model.name, **body),
                timeout=self._timeout_seconds,
            )
        except TimeoutError as exc:
            raise QueryTimeoutError(
                f"A consulta ao dataset '{dataset.name}' excedeu {self._timeout_seconds}s."
            ) from exc

        rows = parse_response(response.body, columns)
        if rows:
            await sink.write(rows)

        return StreamedResult(
            row_count=len(rows),
            total_rows=None,
            execution_ms=response.body["took"],
        )
