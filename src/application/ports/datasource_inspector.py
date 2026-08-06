"""Port de validação semântica do catálogo contra o datasource real.

`docs/pipeline-publicacao.md`: antes de publicar, "confere tabelas/colunas (ou
índice/campos, no caso de elasticsearch) em cada datasource referenciado pelos
datasets". Uma implementação por família de datasource — ver `adapters/catalog/`.
"""

from typing import Protocol, runtime_checkable

from domain.models import Dataset


@runtime_checkable
class DatasourceInspector(Protocol):
    """Confere se o `mapping` físico de um dataset existe de fato no datasource."""

    async def missing_fields(self, dataset: Dataset) -> tuple[str, ...]:
        """Nomes lógicos cujo mapeamento físico (coluna/campo) não foi encontrado no
        datasource — tupla vazia se tudo confere."""
        ...
