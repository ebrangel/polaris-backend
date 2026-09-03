"""Executa uma consulta já enfileirada — o lado worker da seção 2.4.

Não é o `ExecuteQuery`: aquele consulta o cache e enfileira — chamá-lo aqui criaria um
laço. O dataset já foi escolhido por `ResolveDataset` no momento do `enqueue` (seu nome
viajou no payload do job), então este use case nem o `ResolveDataset` chama — só busca o
dataset pelo nome e executa.

**Uma passada só (Marco 12).** Antes, o resultado era materializado e depois lido três
vezes: para o cache, para o CSV e para o valor de retorno do job. Agora o executor lê o
cursor em blocos e cada bloco vai, no mesmo instante, para todos os destinos abertos aqui
— o `FanOutSink`. O que volta pela fila é só o descritor (`QueryResult.streamed`, com
`rows=None`): as linhas ficam no artefato em disco e no cache, e o resultado retido do
`arq` deixa de carregar megabytes.

Este continua sendo o **único escritor do cache de resultados**: como toda consulta passa
pela fila, é aqui que o resultado concluído fica disponível para as requisições idênticas
seguintes.
"""

import logging
from collections.abc import Mapping

from application.ports.cache_gateway import CacheGateway
from application.ports.query_executor import QueryExecutor
from application.ports.result_exporter import ResultExporter
from application.ports.row_sink import RowSink
from application.use_cases._executor_lookup import executor_for
from application.use_cases._fan_out import FanOutSink
from application.use_cases._slow_query_log import log_if_slow
from domain.models import Catalog, Column, QueryRequest, QueryResult

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

        sink = FanOutSink(await self._open_sinks(request, columns, dataset.name))
        try:
            streamed = await executor.execute(dataset, request, columns, sink)
        except BaseException:
            # A consulta falhou no meio: nenhum destino pode ficar visível com um
            # resultado pela metade. `abort` é best-effort e nunca levanta, para não
            # trocar a exceção real da consulta por uma de limpeza.
            await sink.abort()
            raise
        await sink.close(streamed)

        result = QueryResult.streamed(
            query_id=request.query_id,
            columns=columns,
            row_count=streamed.row_count,
            total_rows=streamed.total_rows,
            dataset_used=dataset.name,
            execution_ms=streamed.execution_ms,
        )
        log_if_slow(
            result, schema_name=schema.name, threshold_ms=self._slow_query_threshold_ms
        )
        return result

    async def _open_sinks(
        self, request: QueryRequest, columns: tuple[Column, ...], dataset_name: str
    ) -> list[RowSink]:
        """Abre os destinos configurados.

        Abrir é a única parte que ainda pode falhar antes de a consulta rodar, e aqui a
        falha também é tolerada: um `export_dir` sem permissão de escrita não é motivo
        para o cliente ficar sem a resposta. O que se perde é o artefato, não o
        resultado.

        Todo job concluído ganha um artefato, e isso **não** é condicionado a o cliente
        ter pedido CSV: o formato de saída não faz parte da `QueryRequest` nem do
        `query_id` (seção 2.3a), e o `arq` deduplica jobs por `_job_id=query_id`. Carregar
        a intenção de export no payload faria duas requisições idênticas — uma em JSON,
        outra em CSV — colidirem no mesmo job, e a intenção da segunda se perderia sem
        aviso.
        """
        sinks: list[RowSink] = []

        if self._result_exporter is not None:
            try:
                sinks.append(
                    await self._result_exporter.open_writer(
                        request.query_id, columns, dataset_name
                    )
                )
            except Exception:
                logger.warning(
                    "falha ao abrir o export da consulta %s", request.query_id,
                    exc_info=True,
                )

        if self._cache is not None:
            try:
                sinks.append(
                    await self._cache.open_writer(
                        request.cache_key,
                        columns,
                        request.query_id,
                        dataset_name,
                        self._cache_ttl_seconds,
                    )
                )
            except Exception:
                logger.warning(
                    "falha ao abrir o cache da consulta %s", request.query_id,
                    exc_info=True,
                )

        return sinks
