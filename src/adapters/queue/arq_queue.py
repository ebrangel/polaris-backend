"""`ArqJobQueue` — implementa `JobQueue` (Marco 2) sobre `arq` (fila async-nativa
sobre Redis; ver "Marco 7" no `CLAUDE.md` para o porquê da troca em relação ao Celery
citado ali).
"""

import asyncio
import logging

from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus, ResultNotFound

from adapters.serialization import dict_to_result, request_to_dict
from domain.models import QueryRequest, QueryResult

logger = logging.getLogger(__name__)

#: Estados do arq que ainda não têm resultado — todos viram `status=processing`.
_PENDING = {JobStatus.deferred, JobStatus.queued, JobStatus.in_progress}


class ArqJobQueue:
    """Enfileira consultas via `arq` e acompanha status pelo `query_id`."""

    def __init__(
        self,
        pool: ArqRedis,
        function_name: str = "run_heavy_query",
        queue_name: str = "arq:queue",
        result_poll_delay: float = 0.1,
    ) -> None:
        """`result_poll_delay` é o intervalo com que `wait_for_result` consulta o Redis
        por um único job — bem menor que o `poll_delay` do worker (0.5s padrão), para
        apertar o piso de latência da espera inline da API."""
        self._pool = pool
        self._function_name = function_name
        self._queue_name = queue_name
        self._result_poll_delay = result_poll_delay

    async def enqueue(self, request: QueryRequest, dataset_name: str) -> QueryResult:
        """Usa `query_id` como `_job_id`: duas requisições idênticas (mesmo hash da
        seção 3) reaproveitam o mesmo job — o `arq` não duplica um `_job_id` já na fila
        (`enqueue_job` devolve `None` nesse caso, que aqui é ignorado de propósito: o
        `202` devolvido é sempre o mesmo, exista ou não job novo por trás)."""
        await self._pool.enqueue_job(
            self._function_name,
            request_to_dict(request),
            dataset_name,
            _job_id=request.query_id,
            _queue_name=self._queue_name,
        )
        return QueryResult.processing(request.query_id)

    async def wait_for_result(self, query_id: str, timeout: float) -> QueryResult:
        """Aguarda o job por até `timeout` segundos deixando o `arq` fazer o poll
        (`Job.result`): concluiu → `completed`; levantou → `failed`; estourou o tempo ou
        job ausente → `processing`. Nunca levanta."""
        job = Job(query_id, self._pool, _queue_name=self._queue_name)
        try:
            payload = await job.result(
                timeout=timeout, poll_delay=self._result_poll_delay
            )
        except (TimeoutError, asyncio.TimeoutError):
            return QueryResult.processing(query_id)
        except ResultNotFound:
            # Job fora da fila e sem resultado guardado (`keep_result` expirado, ou o
            # enqueue não pegou) — deixa o GET /v1/query/{query_id} revelar a realidade.
            logger.warning("job %s ausente ao aguardar resultado inline", query_id)
            return QueryResult.processing(query_id)
        except Exception as exc:  # noqa: BLE001 — o arq re-levanta aqui a exceção do worker
            return QueryResult.failed(query_id, error=str(exc))
        return dict_to_result(payload)

    async def get_status(self, query_id: str) -> QueryResult | None:
        job = Job(query_id, self._pool, _queue_name=self._queue_name)
        status = await job.status()

        if status is JobStatus.not_found:
            return None
        if status in _PENDING:
            return QueryResult.processing(query_id)

        # JobStatus.complete
        info = await job.result_info()
        assert info is not None  # `complete` garante que o resultado já foi gravado
        if info.success:
            return dict_to_result(info.result)
        return QueryResult.failed(query_id, error=str(info.result))

    async def depth(self) -> int:
        return await self._pool.zcard(self._queue_name)
