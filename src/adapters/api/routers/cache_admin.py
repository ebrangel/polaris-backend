"""`POST /internal/cache/purge` — limpeza forçada do cache de resultados.

Mesmo padrão de proteção de `admin.py` / `observability.py`: `X-Internal-Token`
registrado como dependência do router inteiro, sem autenticação real ainda.
"""

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from adapters.api.dependencies import PurgeCacheDep, require_internal_token

router = APIRouter(
    prefix="/internal/cache",
    tags=["admin"],
    dependencies=[Depends(require_internal_token)],
)


@router.post("/purge")
async def purge_cache(
    purge_cache: PurgeCacheDep,
    schema: Annotated[
        str | None,
        Query(
            description=(
                "Se informado, limpa só as entradas de cache daquele schema; "
                "omitido, limpa o cache inteiro. Schema inexistente é no-op "
                "(`purged: 0`), não erro."
            )
        ),
    ] = None,
) -> dict[str, Any]:
    purged = await purge_cache(schema=schema)
    return {"purged": purged, "schema": schema}
