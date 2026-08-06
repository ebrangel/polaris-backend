"""`GET /internal/observability` — taxa de acerto de cache e tamanho da fila de
consultas pesadas (Marco 9), mesmo padrão de proteção do `admin.py` (Marco 8):
`X-Internal-Token`, sem autenticação real ainda.
"""

from typing import Any

from fastapi import APIRouter, Depends

from adapters.api.dependencies import ObservabilitySnapshotDep, require_internal_token

router = APIRouter(
    prefix="/internal/observability",
    tags=["observability"],
    dependencies=[Depends(require_internal_token)],
)


@router.get("")
async def get_observability(snapshot_use_case: ObservabilitySnapshotDep) -> dict[str, Any]:
    snapshot = await snapshot_use_case()
    return {
        "cache": {
            "hits": snapshot.cache_hits,
            "misses": snapshot.cache_misses,
            "hit_rate": snapshot.cache_hit_rate,
        },
        "heavy_queue": {"depth": snapshot.heavy_queue_depth},
    }
