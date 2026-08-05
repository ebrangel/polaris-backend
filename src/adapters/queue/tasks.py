"""Shim entre a convenção de chamada do `arq` (`(ctx, *args)`) e `RunQueuedQuery`.

`run_heavy_query` é a task que `ArqJobQueue.enqueue` referencia pelo nome e que o
worker executa para cada job. Ela só desserializa/serializa e delega — a lógica de
negócio é inteiramente do use case, testável sem Redis nem worker algum.
"""

from typing import Any

from adapters.serialization import dict_to_request, result_to_dict
from application.use_cases.run_queued_query import RunQueuedQuery

#: Chave em `ctx` onde `on_startup` (abaixo) deixa o use case para as tasks lerem.
_CTX_KEY = "run_queued_query"


async def run_heavy_query(ctx: dict[str, Any], request_dict: dict, dataset_name: str) -> dict:
    """Task registrada no worker — o nome desta função é o `function_name` padrão de
    `ArqJobQueue` (mesmo string dos dois lados, checado em teste)."""
    run_queued_query: RunQueuedQuery = ctx[_CTX_KEY]
    result = await run_queued_query(dict_to_request(request_dict), dataset_name)
    return result_to_dict(result)


def build_worker_settings(
    run_queued_query: RunQueuedQuery,
    redis_settings: Any,
    queue_name: str = "arq:queue",
) -> type:
    """`WorkerSettings` que `arq.worker.run_worker`/o CLI `arq` esperam.

    Só a forma: fiar com dependências reais (engines por datasource, catálogo lido do
    Postgres) — para popular `run_queued_query` de verdade — é do composition root
    (Marco 8), que chama esta fábrica já com tudo montado.
    """

    async def _on_startup(ctx: dict[str, Any]) -> None:
        ctx[_CTX_KEY] = run_queued_query

    # Nomes diferentes das variáveis externas vs. dos atributos de classe abaixo, de
    # propósito: `atributo = atributo` dentro de um corpo de classe não enxerga a
    # variável de mesmo nome da função externa — o compilador trata o próprio corpo da
    # classe como escopo local para esse nome, e o valor ainda não foi atribuído no
    # momento em que o lado direito é avaliado (`NameError`, não o valor esperado).
    settings_redis, settings_queue_name = redis_settings, queue_name

    class WorkerSettings:
        functions = [run_heavy_query]
        redis_settings = settings_redis
        queue_name = settings_queue_name
        on_startup = staticmethod(_on_startup)

    return WorkerSettings
