"""`ArqJobQueue` — implementa `JobQueue` (Marco 2) sobre `arq` (fila async-nativa
sobre Redis; ver "Marco 7" no `CLAUDE.md` para o porquê da troca em relação ao Celery
citado ali).
"""

from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus

from adapters.serialization import dict_to_result, request_to_dict
from domain.models import QueryRequest, QueryResult

#: Estados do arq que ainda não têm resultado — todos viram `status=processing`.
_PENDING = {JobStatus.deferred, JobStatus.queued, JobStatus.in_progress}


class ArqJobQueue:
    """Enfileira consultas pesadas via `arq` e acompanha status pelo `query_id`."""

    def __init__(
        self,
        pool: ArqRedis,
        function_name: str = "run_heavy_query",
        queue_name: str = "arq:queue",
    ) -> None:
        self._pool = pool
        self._function_name = function_name
        self._queue_name = queue_name

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
