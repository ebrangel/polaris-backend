"""`GetObservabilitySnapshot` — junta as métricas do Marco 9 num só lugar, reaproveitando
os ports que já existiam (`CacheGateway.stats()`, `JobQueue.depth()`); nenhum port novo
só para leitura de métricas.
"""

from dataclasses import dataclass

from application.ports.cache_gateway import CacheGateway
from application.ports.job_queue import JobQueue


@dataclass(frozen=True, slots=True)
class ObservabilitySnapshot:
    """O que `GET /internal/observability` expõe (Marco 9)."""

    cache_hits: int
    cache_misses: int
    cache_hit_rate: float
    heavy_queue_depth: int


class GetObservabilitySnapshot:
    def __init__(self, cache: CacheGateway, job_queue: JobQueue) -> None:
        self._cache = cache
        self._job_queue = job_queue

    async def __call__(self) -> ObservabilitySnapshot:
        stats = await self._cache.stats()
        depth = await self._job_queue.depth()

        total = stats.hits + stats.misses
        hit_rate = stats.hits / total if total else 0.0

        return ObservabilitySnapshot(
            cache_hits=stats.hits,
            cache_misses=stats.misses,
            cache_hit_rate=hit_rate,
            heavy_queue_depth=depth,
        )
