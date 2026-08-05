"""Orquestra o fluxo completo da seção 3 do contrato — só o caminho síncrono.

(1) validar contra o modelo lógico → (2) autorizar → (3) aplicar o teto de `limit` do
schema → (4) checar o cache pelo `query_id` → (5) resolver o dataset → (6) executar →
(7) gravar no cache. Custo estimado e fila (Marco 7) entram depois, de forma aditiva
sobre este mesmo use case — por isso ele já recebe um mapa de executores por engine, em
vez de um único `QueryExecutor`.
"""

from collections.abc import Iterable, Mapping
from dataclasses import replace

from application.ports.cache_gateway import CacheGateway
from application.ports.query_executor import QueryExecutor
from application.use_cases.resolve_dataset import ResolveDataset
from domain.models import Catalog, Dataset, DatasourceType, QueryRequest, QueryResult, QueryStatus


class ExecuteQuery:
    """Ponto de entrada único para `POST` e `GET /v1/query` (Marco 6): os dois convergem
    para o mesmo `QueryRequest` e chamam este use case.
    """

    def __init__(
        self,
        catalog: Catalog,
        resolve_dataset: ResolveDataset,
        executors: Mapping[DatasourceType, QueryExecutor],
        cache: CacheGateway,
        cache_ttl_seconds: int | None = None,
    ) -> None:
        self._catalog = catalog
        self._resolve_dataset = resolve_dataset
        self._executors = executors
        self._cache = cache
        self._cache_ttl_seconds = cache_ttl_seconds

    async def __call__(self, request: QueryRequest, *, roles: Iterable[str]) -> QueryResult:
        schema = self._catalog.get_schema(request.schema)

        schema.validate_request(request)  # passo (1) da seção 3
        schema.authorize(request, roles)  # `forbidden_measure`, antes de tocar o cache

        # Teto de `limit` por schema (seção 2.6), aplicado antes do `query_id`: duas
        # requisições que só diferem num `limit` acima do teto compartilham a mesma
        # entrada de cache, e o `query_id` devolvido corresponde ao que foi executado.
        request = replace(request, limit=schema.effective_limit(request.limit))

        cached = await self._cache.get(request.query_id)
        if cached is not None:
            return replace(cached, meta=replace(cached.meta, cached=True))

        dataset = self._resolve_dataset(schema, request)
        columns = schema.columns_for(request)
        executor = self._executor_for(dataset)

        result = await executor.execute(dataset, request, columns)

        if result.status is QueryStatus.COMPLETED:
            await self._cache.set(request.query_id, result, self._cache_ttl_seconds)

        return result

    def _executor_for(self, dataset: Dataset) -> QueryExecutor:
        engine = dataset.datasource.type
        try:
            return self._executors[engine]
        except KeyError:
            # Fiação incompleta do composition root (Marco 8), não erro do cliente —
            # por isso não é um DomainError da seção 2.5.
            raise LookupError(
                f"Nenhum QueryExecutor configurado para o datasource '{engine.value}' "
                f"(dataset '{dataset.name}')."
            ) from None
