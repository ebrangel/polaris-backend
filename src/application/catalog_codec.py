"""`dict` ↔ `Schema`, JSON canônico e hash — o formato de troca do catálogo.

Fica em `application/`, não em `adapters/`: `dict` é estrutura Python pura, não
framework — o codec é o contrato canônico do catálogo (usado por `PublishCatalog` e
por `PostgresCatalogRepository`, que reusa este módulo em vez de duplicá-lo). PyYAML
entra só no adapter que lê o arquivo (`adapters/catalog/yaml_loader.py`); aqui a
entrada já é o `dict` resultante de `yaml.safe_load`.

O formato do `dict` espelha exatamente o YAML da seção 1 de
`docs/catalogo-e-contrato-completo.md` — `yaml.safe_load` de um dos três exemplos do
documento produz, sem nenhuma tradução extra, o `dict` que `schema_from_dict` espera.
"""

import hashlib
import json
from collections.abc import Mapping

from domain.errors import InvalidCatalogError
from domain.models import (
    AccessControl,
    Aggregation,
    ColumnMapping,
    Dataset,
    Datasource,
    DatasourceType,
    DataType,
    Dimension,
    DimensionTable,
    Fact,
    FactKey,
    FieldMapping,
    IndexModel,
    Join,
    Measure,
    PhysicalModel,
    Provides,
    Schema,
    StarModel,
    TableModel,
)

__all__ = [
    "canonical_json",
    "compile_schema",
    "content_hash",
    "decompile_schema",
    "schema_from_dict",
    "schema_to_dict",
]


# --------------------------------------------------------------------------------------
# dict -> Schema
# --------------------------------------------------------------------------------------


def schema_from_dict(data: Mapping) -> Schema:
    """`dict` (de `yaml.safe_load` ou de `content` já gravado) → `Schema` validado.

    As invariantes de domínio (Marco 1) rodam na construção do `Schema`/`Dataset` e
    sobem como `InvalidCatalogError` sem tradução — só chaves ausentes/tipos errados
    na *forma* do dict (não nas regras de negócio) são traduzidas aqui.
    """
    try:
        return _schema_from_dict(data)
    except KeyError as exc:
        raise InvalidCatalogError(f"Campo obrigatório ausente no catálogo: {exc}.") from exc
    except (TypeError, ValueError) as exc:
        raise InvalidCatalogError(f"Catálogo malformado: {exc}.") from exc


def _schema_from_dict(data: Mapping) -> Schema:
    dimensions = {
        name: Dimension(
            name=name,
            type=DataType(d.get("type", DataType.STRING.value)),
            filterable=d.get("filterable", True),
        )
        for name, d in data.get("dimensions", {}).items()
    }
    measures = {
        name: Measure(name=name, agg=Aggregation(m["agg"]), format=m.get("format"))
        for name, m in data.get("measures", {}).items()
    }

    access_control = None
    if "access_control" in data:
        access_control = AccessControl(roles=data["access_control"].get("roles", {}))

    datasets = tuple(_dataset_from_dict(d) for d in data.get("datasets", []))

    return Schema(
        name=data["schema"],
        version=data["version"],
        dimensions=dimensions,
        measures=measures,
        datasets=datasets,
        description=data.get("description"),
        access_control=access_control,
        max_limit=data.get("max_limit"),
    )


def _dataset_from_dict(data: Mapping) -> Dataset:
    datasource = Datasource(
        type=DatasourceType(data["datasource"]["type"]),
        connection_ref=data["datasource"]["connection_ref"],
    )
    provides = Provides(
        dimensions=frozenset(data["provides"].get("dimensions", [])),
        measures=frozenset(data["provides"].get("measures", [])),
    )
    return Dataset(
        name=data["name"],
        datasource=datasource,
        provides=provides,
        model=_physical_model_from_dict(data),
    )


def _physical_model_from_dict(data: Mapping) -> PhysicalModel:
    """`data` é o dict de **um** dataset — `table`/`index`/`fact` são mutuamente
    exclusivos, exatamente como nos exemplos das seções 1.0/1.1/1.2."""
    if "table" in data:
        return TableModel(
            source=data["table"]["source"],
            mapping=_column_mapping_from_dict(data["table"]["mapping"]),
        )
    if "index" in data:
        return IndexModel(
            name=data["index"]["name"],
            mapping=_field_mapping_from_dict(data["index"]["mapping"]),
        )
    if "fact" in data:
        fact_data = data["fact"]
        fact = Fact(
            table=fact_data["table"],
            mapping=_column_mapping_from_dict(fact_data["mapping"]),
            keys={
                key_name: FactKey(column=k["column"], references=k["references"])
                for key_name, k in fact_data.get("keys", {}).items()
            },
        )
        dimension_tables = {
            alias: DimensionTable(
                table=dim["table"],
                primary_key=dim["primary_key"],
                mapping=_column_mapping_from_dict(dim["mapping"]),
            )
            for alias, dim in data.get("dimensions", {}).items()
        }
        joins = tuple(Join(from_ref=j["from"], to_ref=j["to"]) for j in data.get("joins", []))
        return StarModel(fact=fact, dimension_tables=dimension_tables, joins=joins)

    raise InvalidCatalogError(
        f"O dataset '{data.get('name', '?')}' não declara `table`, `index` nem `fact`.",
        [data.get("name", "?")],
    )


def _column_mapping_from_dict(mapping: Mapping) -> dict[str, ColumnMapping]:
    return {
        name: ColumnMapping(column=m["column"], agg=Aggregation(m["agg"]) if "agg" in m else None)
        for name, m in mapping.items()
    }


def _field_mapping_from_dict(mapping: Mapping) -> dict[str, FieldMapping]:
    return {
        name: FieldMapping(
            field=m["field"],
            es_type=m.get("es_type"),
            agg=Aggregation(m["agg"]) if "agg" in m else None,
        )
        for name, m in mapping.items()
    }


# --------------------------------------------------------------------------------------
# Schema -> dict
# --------------------------------------------------------------------------------------


def schema_to_dict(schema: Schema) -> dict:
    """Inverso de `schema_from_dict`. Frozensets/dicts viram listas **ordenadas** —
    obrigatório para que `canonical_json` produza o mesmo hash em toda execução (a
    ordem de iteração de um `set` de strings não é garantida entre processos)."""
    data: dict = {
        "version": schema.version,
        "schema": schema.name,
        "dimensions": {
            name: {"type": dim.type.value, "filterable": dim.filterable}
            for name, dim in schema.dimensions.items()
        },
        "measures": {name: _measure_to_dict(m) for name, m in schema.measures.items()},
        "datasets": [_dataset_to_dict(d) for d in schema.datasets],
    }
    if schema.description is not None:
        data["description"] = schema.description
    if schema.access_control is not None:
        data["access_control"] = {
            "roles": {
                role: sorted(measures)
                for role, measures in schema.access_control.roles.items()
            }
        }
    if schema.max_limit is not None:
        data["max_limit"] = schema.max_limit
    return data


def _measure_to_dict(measure: Measure) -> dict:
    data: dict = {"agg": measure.agg.value}
    if measure.format is not None:
        data["format"] = measure.format
    return data


def _dataset_to_dict(dataset: Dataset) -> dict:
    data: dict = {
        "name": dataset.name,
        "datasource": {
            "type": dataset.datasource.type.value,
            "connection_ref": dataset.datasource.connection_ref,
        },
        "provides": {
            "dimensions": sorted(dataset.provides.dimensions),
            "measures": sorted(dataset.provides.measures),
        },
    }
    data.update(_physical_model_to_dict(dataset.model))
    return data


def _physical_model_to_dict(model: PhysicalModel) -> dict:
    if isinstance(model, TableModel):
        return {"table": {"source": model.source, "mapping": _column_mapping_to_dict(model.mapping)}}
    if isinstance(model, IndexModel):
        return {"index": {"name": model.name, "mapping": _field_mapping_to_dict(model.mapping)}}

    assert isinstance(model, StarModel)
    return {
        "fact": {
            "table": model.fact.table,
            "mapping": _column_mapping_to_dict(model.fact.mapping),
            "keys": {
                name: {"column": k.column, "references": k.references}
                for name, k in model.fact.keys.items()
            },
        },
        "dimensions": {
            alias: {
                "table": dim.table,
                "primary_key": dim.primary_key,
                "mapping": _column_mapping_to_dict(dim.mapping),
            }
            for alias, dim in model.dimension_tables.items()
        },
        "joins": [{"from": j.from_ref, "to": j.to_ref} for j in model.joins],
    }


def _column_mapping_to_dict(mapping: Mapping[str, ColumnMapping]) -> dict:
    result: dict = {}
    for name, m in mapping.items():
        item: dict = {"column": m.column}
        if m.agg is not None:
            item["agg"] = m.agg.value
        result[name] = item
    return result


def _field_mapping_to_dict(mapping: Mapping[str, FieldMapping]) -> dict:
    result: dict = {}
    for name, m in mapping.items():
        item: dict = {"field": m.field}
        if m.es_type is not None:
            item["es_type"] = m.es_type
        if m.agg is not None:
            item["agg"] = m.agg.value
        result[name] = item
    return result


# --------------------------------------------------------------------------------------
# JSON canônico + hash
# --------------------------------------------------------------------------------------


def canonical_json(data: Mapping) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def compile_schema(data: Mapping) -> tuple[Schema, str, str]:
    """`data` (YAML já carregado) → (`Schema` validado, JSON canônico, hash).

    O hash é do **compilado**, não do YAML cru: reordenar chaves, reindentar ou
    acrescentar uma chave não modelada não muda o hash — é isso que faz a comparação
    incremental da publicação (`docs/pipeline-publicacao.md`) ser estável.
    """
    schema = schema_from_dict(data)
    content = canonical_json(schema_to_dict(schema))
    return schema, content, content_hash(content)


def decompile_schema(content: str) -> Schema:
    """`content` (já gravado em `catalog_versions`) → `Schema`. Usado pelo
    `CatalogRepository` ao reconstruir uma versão lida do banco."""
    return schema_from_dict(json.loads(content))
