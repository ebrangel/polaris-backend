"""`RedisCacheGateway` — implementa `CacheGateway` (Marco 2) sobre `redis.asyncio`.

A chave é `QueryRequest.cache_key` (`<schema>:<query_id>`); aqui só se soma o prefixo
`query:`, tanto para não colidir com o namespace `arq:*` da fila (`ArqJobQueue`) e o
`ratelimit:request:*` do rate limiter no mesmo Redis, quanto para que `clear(schema)`
seja um `SCAN MATCH query:<schema>:*`.
"""

import json
import logging
from collections.abc import Sequence
from typing import Any

from redis.asyncio import Redis

from adapters.serialization import dict_to_result, jsonable
from application.ports.cache_gateway import CacheStats
from application.ports.row_sink import StreamedResult
from domain.models import Column, QueryResult

#: Chaves fixas, sem TTL — contadores acumulados desde o boot do processo (Marco 9),
#: não por janela de tempo (isso é o rate limiter, não a taxa de acerto de cache).
_HITS_KEY = "cache:hits"
_MISSES_KEY = "cache:misses"

logger = logging.getLogger(__name__)


class RedisCacheGateway:
    """Cache de `QueryResult` por `query_id`, com TTL padrão configurável."""

    def __init__(
        self,
        client: Redis,
        key_prefix: str = "query:",
        default_ttl_seconds: int | None = 3600,
        max_rows: int | None = 100_000,
        max_payload_bytes: int | None = 8 * 1024 * 1024,
    ) -> None:
        """`max_rows`/`max_payload_bytes` são o teto do que vale a pena cachear.

        Não é uma decisão de negócio (por isso mora aqui, e não no use case): é a
        realidade do Redis, que guarda cada valor inteiro em memória e recusa valores
        acima do seu próprio limite. Um resultado gigante gravado a cada consulta
        expulsaria do cache todas as consultas pequenas — que são justamente as que se
        repetem e as que o cache existe para acelerar.

        Os dois tetos são checados em momentos diferentes de propósito: `max_rows`
        **antes** de serializar (é o que evita montar um JSON de centenas de MB só para
        descobrir que ele não cabe) e `max_payload_bytes` depois, sobre o tamanho real.
        `None` em qualquer um deles desliga aquele teto.
        """
        self._client = client
        self._key_prefix = key_prefix
        self._default_ttl_seconds = default_ttl_seconds
        self._max_rows = max_rows
        self._max_payload_bytes = max_payload_bytes

    def _redis_key(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    async def get(self, key: str) -> QueryResult | None:
        raw = await self._client.get(self._redis_key(key))
        if raw is None:
            await self._client.incr(_MISSES_KEY)
            return None
        await self._client.incr(_HITS_KEY)
        return dict_to_result(json.loads(raw))

    async def open_writer(
        self,
        key: str,
        columns: tuple[Column, ...],
        query_id: str,
        dataset_used: str,
        ttl_seconds: int | None = None,
    ) -> "_RedisCacheSink":
        return _RedisCacheSink(
            client=self._client,
            redis_key=self._redis_key(key),
            log_key=key,
            columns=columns,
            query_id=query_id,
            dataset_used=dataset_used,
            ttl_seconds=(
                ttl_seconds if ttl_seconds is not None else self._default_ttl_seconds
            ),
            max_rows=self._max_rows,
            max_payload_bytes=self._max_payload_bytes,
        )

    async def delete(self, key: str) -> None:
        await self._client.delete(self._redis_key(key))

    async def clear(self, schema: str | None = None) -> int:
        """Varre por padrão e apaga em lotes — não há registro de chaves para consultar.

        `cache:hits`/`cache:misses` não têm o prefixo `query:`, então nunca casam; o
        mesmo vale para `arq:*` e `ratelimit:request:*` no mesmo Redis.
        """
        match = (
            f"{self._key_prefix}{schema}:*"
            if schema is not None
            else f"{self._key_prefix}*"
        )
        removed = 0
        batch: list[str] = []
        async for redis_key in self._client.scan_iter(match=match, count=500):
            batch.append(redis_key)
            if len(batch) >= 500:
                removed += await self._client.delete(*batch)
                batch.clear()
        if batch:
            removed += await self._client.delete(*batch)
        return removed

    async def stats(self) -> CacheStats:
        hits, misses = await self._client.mget(_HITS_KEY, _MISSES_KEY)
        return CacheStats(hits=int(hits or 0), misses=int(misses or 0))


class _RedisCacheSink:
    """Acumula as linhas serializadas e grava a entrada de cache num `SET` no `close`.

    **O teto é o pico de memória, por construção.** O Redis guarda cada valor inteiro na
    memória e recusa valores acima do seu próprio limite; um resultado gigante gravado a
    cada consulta expulsaria do cache todas as consultas pequenas — que são justamente as
    que se repetem e as que o cache existe para acelerar. Então, em vez de acompanhar o
    cursor até o fim para só então descobrir que não cabe, este sink desiste no instante
    em que passa do teto e **libera o buffer**: daí em diante os blocos são descartados
    conforme chegam, e o custo de memória para de crescer.

    Desistir não é erro. O port promete gravar o que couber; a consulta seguinte
    simplesmente executa de novo. Levantar exceção aqui transformaria um limite de
    operação em falha de requisição — e, pior, derrubaria um job cujo resultado já está
    calculado e já foi para o disco.
    """

    __slots__ = (
        "_client", "_redis_key", "_log_key", "_columns", "_query_id", "_dataset_used",
        "_ttl_seconds", "_max_rows", "_max_payload_bytes",
        "_rows", "_row_count", "_payload_bytes", "_abandoned", "_closed",
    )

    def __init__(
        self,
        *,
        client: Redis,
        redis_key: str,
        log_key: str,
        columns: tuple[Column, ...],
        query_id: str,
        dataset_used: str,
        ttl_seconds: int | None,
        max_rows: int | None,
        max_payload_bytes: int | None,
    ) -> None:
        self._client = client
        self._redis_key = redis_key
        self._log_key = log_key
        self._columns = columns
        self._query_id = query_id
        self._dataset_used = dataset_used
        self._ttl_seconds = ttl_seconds
        self._max_rows = max_rows
        self._max_payload_bytes = max_payload_bytes

        self._rows: list[str] = []
        self._row_count = 0
        self._payload_bytes = 0
        self._abandoned = False
        self._closed = False

    def _give_up(self, motivo: str) -> None:
        self._abandoned = True
        self._rows = []
        logger.info("resultado de %s não cacheado: %s", self._log_key, motivo)

    async def write(self, rows: Sequence[tuple[Any, ...]]) -> None:
        if self._abandoned or self._closed:
            return

        self._row_count += len(rows)
        if self._max_rows is not None and self._row_count > self._max_rows:
            self._give_up(f"{self._row_count} linhas acima do teto de {self._max_rows}")
            return

        for row in rows:
            # `ensure_ascii` fica no padrão (`True`) de propósito: com o payload em ASCII
            # puro, `len()` da string é exatamente o tamanho em bytes, e o teto é medido
            # sem pagar uma cópia extra do texto só para chamar `encode()`.
            serialized = json.dumps([jsonable(value) for value in row])
            self._payload_bytes += len(serialized) + 1  # +1 pela vírgula que os separa
            if (
                self._max_payload_bytes is not None
                and self._payload_bytes > self._max_payload_bytes
            ):
                self._give_up(
                    f"{self._payload_bytes} bytes acima do teto de {self._max_payload_bytes}"
                )
                return
            self._rows.append(serialized)

    async def close(self, result: StreamedResult) -> None:
        if self._closed:
            return
        self._closed = True
        if self._abandoned:
            return

        # O documento é montado por concatenação, e não com um `json.dumps` de um dict
        # pronto: as linhas já estão serializadas desde o `write`, e desserializá-las
        # para reserializar tudo junto recriaria em memória exatamente o resultado
        # inteiro que este caminho existe para não construir.
        payload = (
            '{"query_id":' + json.dumps(self._query_id)
            + ',"status":"completed"'
            + ',"columns":' + json.dumps(
                [
                    {"field": c.field, "type": c.type.value, "format": c.format}
                    for c in self._columns
                ]
            )
            + ',"rows":[' + ",".join(self._rows) + "]"
            + ',"meta":' + json.dumps(
                {
                    "row_count": result.row_count,
                    "cached": False,
                    "execution_ms": result.execution_ms,
                    "dataset_used": self._dataset_used,
                    "total_rows": result.total_rows,
                }
            )
            + "}"
        )
        self._rows = []
        await self._client.set(self._redis_key, payload, ex=self._ttl_seconds)

    async def abort(self) -> None:
        self._closed = True
        self._rows = []
