"""Log de consultas lentas (Marco 9) — compartilhado entre `ExecuteQuery` e
`RunQueuedQuery`, os dois pontos onde uma consulta de verdade roda contra um
datasource. Mesmo padrão de `_executor_lookup.py`: helper privado, não um port —
`logging` é stdlib, não é I/O de infraestrutura que precise ser trocado por um fake.
"""

import logging

from domain.models import QueryResult, QueryStatus

logger = logging.getLogger(__name__)


def log_if_slow(result: QueryResult, *, schema_name: str, threshold_ms: int | None) -> None:
    """Loga em `WARNING` quando `result` está concluído e levou `threshold_ms` ou mais.

    `threshold_ms=None` desliga o log inteiramente — é o valor por omissão de quem
    constrói `ExecuteQuery`/`RunQueuedQuery` sem configurar o limiar (Marco 4-8, sem
    mudança de comportamento)."""
    if threshold_ms is None or result.status is not QueryStatus.COMPLETED:
        return
    assert result.meta is not None  # garantido pela invariante de QueryResult

    if result.meta.execution_ms >= threshold_ms:
        logger.warning(
            "consulta lenta: query_id=%s schema=%s dataset=%s execution_ms=%d row_count=%d",
            result.query_id,
            schema_name,
            result.meta.dataset_used,
            result.meta.execution_ms,
            result.meta.row_count,
        )
