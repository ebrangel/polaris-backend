"""`DatasourceInspector` real para Elasticsearch, via `indices.get_mapping`."""

from elasticsearch import AsyncElasticsearch, NotFoundError

from domain.models import Dataset, IndexModel


class ElasticsearchInspector:
    """Implementa `DatasourceInspector` consultando o mapping do índice."""

    def __init__(self, client: AsyncElasticsearch) -> None:
        self._client = client

    async def missing_fields(self, dataset: Dataset) -> tuple[str, ...]:
        assert isinstance(dataset.model, IndexModel)
        index_name = dataset.model.name

        try:
            response = await self._client.indices.get_mapping(index=index_name)
        except NotFoundError:
            # Índice inexistente: todo campo do mapping está "faltando".
            return tuple(dataset.model.mapping)

        index_body = response.body[index_name]
        properties = index_body.get("mappings", {}).get("properties", {})

        return tuple(
            logical_name
            for logical_name, field_mapping in dataset.model.mapping.items()
            if field_mapping.field not in properties
        )
