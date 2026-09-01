"""Executa uma consulta já enfileirada — o lado worker da seção 2.4.

Não é o `ExecuteQuery`: aquele consulta o cache e enfileira — chamá-lo aqui criaria um
laço. O dataset já foi escolhido por `ResolveDataset` no momento do `enqueue` (seu nome
viajou no payload do job), então este use case nem o `ResolveDataset` chama — só busca o
dataset pelo nome e executa.

É também o **único escritor do cache de resultados**: como toda consulta passa pela
fila, é aqui que o resultado concluído é gravado no `CacheGateway` para as requisições
idênticas seguintes.
"""

import logging
from collections.abc import Mapping

from application.ports.cache_gateway import CacheGateway
from application.ports.query_executor import QueryExecutor
from application.ports.result_exporter import ResultExporter
from application.use_cases._executor_lookup import executor_for
from application.use_cases._slow_query_log import log_if_slow
from domain.models import Catalog, QueryRequest, QueryResult, QueryStatus

logger = logging.getLogger(__name__)


class RunQueuedQuery:
    """Chamado pelo worker (`adapters/queue/tasks.py`) para cada job que sai da fila."""

    def __init__(
        self,
        catalog: Catalog,
        executors: Mapping[str, QueryExecutor],
        slow_query_threshold_ms: int | None = None,
        result_exporter: ResultExporter | None = None,
        cache: CacheGateway | None = None,
        cache_ttl_seconds: int | None = None,
    ) -> None:
        """`cache` e `result_exporter` são opcionais pelo mesmo motivo de
        `slow_query_threshold_ms` (Marco 9): sem eles a consulta roda e o resultado
        volta pela fila do mesmo jeito — só não fica um arquivo baixável para trás, nem
        entra no cache."""
        self._catalog = catalog
        self._executors = executors
        self._slow_query_threshold_ms = slow_query_threshold_ms
        self._result_exporter = result_exporter
        self._cache = cache
        self._cache_ttl_seconds = cache_ttl_seconds

    async def __call__(self, request: QueryRequest, dataset_name: str) -> QueryResult:
        schema = self._catalog.get_schema(request.schema)
        dataset = schema.get_dataset(dataset_name)
        columns = schema.columns_for(request)
        executor = executor_for(self._executors, dataset)

        result = await executor.execute(dataset, request, columns)
        log_if_slow(
            result, schema_name=schema.name, threshold_ms=self._slow_query_threshold_ms
        )
        await self._cache_result(request, result)
        await self._export(result)
        return result

    async def _cache_result(self, request: QueryRequest, result: QueryResult) -> None:
        """Gravar no cache é otimização — nunca pode derrubar um job cujo resultado já
        existe. Mesmo contrato best-effort do `_export`: um resultado grande demais para
        o Redis, uma indisponibilidade momentânea ou um teto do adapter custam, no
        máximo, um acerto de cache na próxima requisição igual.
        """
        if self._cache is None or result.status is not QueryStatus.COMPLETED:
            return
        try:
            await self._cache.set(request.cache_key, result, self._cache_ttl_seconds)
        except Exception:
            logger.warning(
                "falha ao gravar a consulta %s no cache", request.query_id, exc_info=True
            )

    async def _export(self, result: QueryResult) -> None:
        """Grava o arquivo baixável — **toda** consulta concluída ganha um.

        Não é condicionado a o cliente ter pedido CSV, e isso é proposital: o formato de
        saída não faz parte da `QueryRequest` nem do `query_id` (seção 2.3a), e o `arq`
        deduplica jobs por `_job_id=query_id`. Carregar a intenção de export no payload
        faria duas requisições idênticas — uma em JSON, outra em CSV — colidirem no mesmo
        job, e a intenção da segunda se perderia sem aviso. Escrever sempre custa um
        arquivo a mais e elimina a classe de bug inteira.

        Falha de export não derruba o job: o resultado já foi calculado e vale pela fila
        do mesmo jeito — o cliente perde o link de download, não a resposta.
        """
        if self._result_exporter is None or result.status is not QueryStatus.COMPLETED:
            return
        try:
            await self._result_exporter.export(result)
        except Exception:
            logger.warning(
                "falha ao exportar o resultado da consulta %s", result.query_id,
                exc_info=True,
            )
