"""Shim entre a convenção de chamada do `arq` (`(ctx, *args)`) e `RunQueuedQuery`.

`run_heavy_query` é a task que `ArqJobQueue.enqueue` referencia pelo nome e que o
worker executa para cada job. Ela só desserializa/serializa e delega — a lógica de
negócio é inteiramente do use case, testável sem Redis nem worker algum.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from arq.cron import cron

from adapters.serialization import dict_to_request, result_to_dict
from application.ports.result_exporter import ResultExporter
from application.use_cases.run_queued_query import RunQueuedQuery

#: Chave em `ctx` onde `on_startup` (abaixo) deixa o use case para as tasks lerem.
_CTX_KEY = "run_queued_query"

#: Idem para o exportador, que a varredura periódica usa (seção 2.4a).
_EXPORTER_CTX_KEY = "result_exporter"


async def run_heavy_query(ctx: dict[str, Any], request_dict: dict, dataset_name: str) -> dict:
    """Task registrada no worker — o nome desta função é o `function_name` padrão de
    `ArqJobQueue` (mesmo string dos dois lados, checado em teste)."""
    run_queued_query: RunQueuedQuery = ctx[_CTX_KEY]
    result = await run_queued_query(dict_to_request(request_dict), dataset_name)
    return result_to_dict(result)


async def purge_exports(ctx: dict[str, Any]) -> int:
    """Varre os exports vencidos (seção 2.4a) — registrada como cron, não como job.

    Roda no worker, e não na API, porque é o worker que escreve os arquivos: em deploy
    com várias instâncias de API e uma de worker, isso mantém uma varredura só.
    """
    result_exporter: ResultExporter = ctx[_EXPORTER_CTX_KEY]
    return await result_exporter.purge_expired()


def build_worker_settings(
    run_queued_query: RunQueuedQuery | None = None,
    redis_settings: Any = None,
    queue_name: str = "arq:queue",
    *,
    run_queued_query_provider: Callable[[], Awaitable[RunQueuedQuery]] | None = None,
    result_exporter: ResultExporter | None = None,
    result_exporter_provider: Callable[[], Awaitable[ResultExporter]] | None = None,
) -> type:
    """`WorkerSettings` que `arq.worker.run_worker`/o CLI `arq` esperam.

    Só a forma: fiar com dependências reais (engines por datasource, catálogo lido do
    Postgres) — para popular `run_queued_query` de verdade — é do composition root
    (Marco 8), que chama esta fábrica já com tudo montado.

    O composition root passa `run_queued_query_provider` em vez do use case pronto:
    montá-lo exige I/O (ler o catálogo do Postgres) e o CLI `arq main.WorkerSettings`
    importa o módulo antes de existir event loop algum. O provider é aguardado dentro do
    `on_startup`, já no loop do worker. Quem monta o use case sem I/O — os testes —
    continua passando `run_queued_query` direto.

    `result_exporter`/`result_exporter_provider` seguem o mesmo par, e são opcionais: só
    quando um deles é dado é que a varredura de exports entra em `cron_jobs`. Sem
    exportador não há arquivo para limpar, e registrar um cron que só levantaria
    `KeyError` no `ctx` seria pior que não registrar nada.
    """
    if (run_queued_query is None) == (run_queued_query_provider is None):
        raise ValueError(
            "Informe exatamente um entre `run_queued_query` e `run_queued_query_provider`."
        )
    if result_exporter is not None and result_exporter_provider is not None:
        raise ValueError(
            "Informe no máximo um entre `result_exporter` e `result_exporter_provider`."
        )

    exports_enabled = result_exporter is not None or result_exporter_provider is not None

    async def _on_startup(ctx: dict[str, Any]) -> None:
        ctx[_CTX_KEY] = (
            run_queued_query
            if run_queued_query_provider is None
            else await run_queued_query_provider()
        )
        if result_exporter is not None:
            ctx[_EXPORTER_CTX_KEY] = result_exporter
        elif result_exporter_provider is not None:
            ctx[_EXPORTER_CTX_KEY] = await result_exporter_provider()

    # Nomes diferentes das variáveis externas vs. dos atributos de classe abaixo, de
    # propósito: `atributo = atributo` dentro de um corpo de classe não enxerga a
    # variável de mesmo nome da função externa — o compilador trata o próprio corpo da
    # classe como escopo local para esse nome, e o valor ainda não foi atribuído no
    # momento em que o lado direito é avaliado (`NameError`, não o valor esperado).
    settings_redis, settings_queue_name = redis_settings, queue_name

    # De hora em hora, e uma vez no boot: um arquivo vencido já deixa de ser servido no
    # instante em que vence (é o `stat()` do exportador que decide), então a varredura
    # só precisa dar conta do espaço em disco, não da correção da resposta.
    settings_cron_jobs = (
        [cron(purge_exports, minute=0, run_at_startup=True)] if exports_enabled else []
    )

    class WorkerSettings:
        functions = [run_heavy_query]
        cron_jobs = settings_cron_jobs
        redis_settings = settings_redis
        queue_name = settings_queue_name
        on_startup = staticmethod(_on_startup)

    return WorkerSettings
