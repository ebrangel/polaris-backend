"""`FanOutSink` — entrega o mesmo bloco de linhas a vários destinos numa passada só.

Antes do Marco 12 as linhas eram lidas três vezes do `QueryResult` já materializado: uma
para o cache, uma para o CSV, uma para o valor de retorno do job. Um cursor só pode ser
drenado uma vez, então o caminho de streaming precisa distribuir cada bloco no momento em
que ele chega.

Mora em `application/` porque não conhece formato algum: opera sobre o Protocol `RowSink`
e nunca sobre CSV, JSON ou Redis. É a mesma razão de o port receber tuplas de domínio.
"""

import logging
from collections.abc import Sequence
from typing import Any

from application.ports.row_sink import RowSink, StreamedResult

logger = logging.getLogger(__name__)


class FanOutSink:
    """Um `RowSink` que replica as escritas em vários outros.

    **Um destino que falha não derrubar os demais é o ponto do desenho.** O resultado da
    consulta já foi calculado — o caro já foi pago —, então perder o cache ou o arquivo é
    perder uma otimização, não a resposta. É o mesmo contrato best-effort que o
    `RunQueuedQuery` já aplicava a cache e export separadamente, agora num lugar só: o
    sink que falha é descartado, o erro vai para o log, e os outros seguem recebendo.
    """

    __slots__ = ("_sinks", "_failed")

    def __init__(self, sinks: Sequence[RowSink]) -> None:
        self._sinks = list(sinks)
        self._failed = False

    @property
    def degraded(self) -> bool:
        """Se algum destino caiu no caminho — para quem quiser registrar isso."""
        return self._failed

    async def write(self, rows: Sequence[tuple[Any, ...]]) -> None:
        for sink in list(self._sinks):
            try:
                await sink.write(rows)
            except Exception:
                await self._drop(sink, "escrever em")

    async def close(self, result: StreamedResult) -> None:
        for sink in list(self._sinks):
            try:
                await sink.close(result)
            except Exception:
                await self._drop(sink, "fechar")

    async def abort(self) -> None:
        for sink in list(self._sinks):
            try:
                await sink.abort()
            except Exception:
                # `abort` roda em caminho de erro: uma segunda exceção aqui só esconderia
                # a primeira, que é a que interessa a quem for investigar.
                logger.warning(
                    "falha ao abortar o destino %s", type(sink).__name__, exc_info=True
                )
        self._sinks.clear()

    async def _drop(self, sink: RowSink, acao: str) -> None:
        self._failed = True
        self._sinks.remove(sink)
        logger.warning(
            "falha ao %s o destino %s — os demais seguem",
            acao,
            type(sink).__name__,
            exc_info=True,
        )
        # Abortar o destino que caiu é o que impede um temporário órfão de ficar no
        # `export_dir` a cada falha, acumulando até o disco encher.
        try:
            await sink.abort()
        except Exception:
            logger.warning(
                "falha ao abortar o destino %s depois do erro", type(sink).__name__,
                exc_info=True,
            )
