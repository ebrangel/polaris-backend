"""Passo (2) do fluxo da seção 3: escolher a fonte física que atende a requisição.

Não faz I/O — opera só sobre um `Schema` já carregado em memória — por isso é síncrono
(ver "Convenção de assincronia" no `CLAUDE.md`). O passo (1), validar a requisição
contra o modelo lógico, é responsabilidade de `Schema.validate_request()`, chamado pelo
`ExecuteQuery` (Marco 4) antes deste use case — `ResolveDataset` não o repete.
"""

from domain.errors import NoDatasetAvailableError
from domain.models import Dataset, QueryRequest, Schema


class ResolveDataset:
    """Percorre `schema.datasets` em ordem de declaração e devolve o primeiro que cobre
    a requisição — dimensões, medidas e campos usados só em filtro ou ordenação.

    Não há cálculo de "melhor" opção (seção 1.0): a ordem de declaração no catálogo é a
    própria política de otimização, e este use case nunca a reordena nem combina dois
    datasets para atender uma única requisição.
    """

    def __call__(self, schema: Schema, request: QueryRequest) -> Dataset:
        dimensions, measures = schema.split_fields(request.referenced_fields())

        for dataset in schema.datasets:
            if dataset.covers(dimensions, measures):
                return dataset

        raise NoDatasetAvailableError.for_request(
            schema.name, request.referenced_fields_in_order()
        )
