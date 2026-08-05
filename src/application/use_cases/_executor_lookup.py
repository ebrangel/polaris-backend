"""Roteamento por engine — compartilhado entre `ExecuteQuery` e `RunQueuedQuery`.

Módulo privado (prefixo `_`): não faz parte da API pública de `use_cases/`, só existe
para não duplicar esta busca nos dois use cases que precisam dela.
"""

from collections.abc import Mapping

from application.ports.query_executor import QueryExecutor
from domain.models import Dataset, DatasourceType


def executor_for(
    executors: Mapping[DatasourceType, QueryExecutor], dataset: Dataset
) -> QueryExecutor:
    engine = dataset.datasource.type
    try:
        return executors[engine]
    except KeyError:
        # Fiação incompleta do composition root (Marco 8), não erro do cliente — por
        # isso não é um DomainError da seção 2.5.
        raise LookupError(
            f"Nenhum QueryExecutor configurado para o datasource '{engine.value}' "
            f"(dataset '{dataset.name}')."
        ) from None
