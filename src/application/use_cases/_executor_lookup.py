"""Roteamento por engine — compartilhado entre `ExecuteQuery` e `RunQueuedQuery`.

Chaveado por `connection_ref`, não por `DatasourceType`: `connection_ref` é por
**dataset**, não por engine — os exemplos do documento têm dois Postgres distintos
(`env:DW_VENDAS_PG_URL`, seção 1.0, e `env:APP_ESTOQUE_URL`, seção 1.2). Rotear por
`DatasourceType` faria os dois compartilharem engine e pool, contrariando
`docs/escalabilidade.md`: "nunca compartilhar o mesmo pool ... entre datasources".

Módulo privado (prefixo `_`): não faz parte da API pública de `use_cases/`, só existe
para não duplicar esta busca nos dois use cases que precisam dela.
"""

from collections.abc import Mapping

from application.ports.query_executor import QueryExecutor
from domain.models import Dataset


def executor_for(executors: Mapping[str, QueryExecutor], dataset: Dataset) -> QueryExecutor:
    connection_ref = dataset.datasource.connection_ref
    try:
        return executors[connection_ref]
    except KeyError:
        # Fiação incompleta do composition root (Marco 8), não erro do cliente — por
        # isso não é um DomainError da seção 2.5.
        raise LookupError(
            f"Nenhum QueryExecutor configurado para connection_ref '{connection_ref}' "
            f"(dataset '{dataset.name}')."
        ) from None
