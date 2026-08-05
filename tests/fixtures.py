"""Catálogos dos exemplos de `docs/catalogo-e-contrato-completo.md`, como objetos de domínio.

Cada função reproduz literalmente um YAML da documentação — nomes lógicos, tabelas,
colunas e ordem de declaração dos datasets são os mesmos do documento.
"""

from domain.models import (
    AccessControl,
    Aggregation,
    Catalog,
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
    Provides,
    Schema,
    StarModel,
    TableModel,
)


def vendas_agregado_uf() -> Dataset:
    """Primeiro dataset do schema `vendas` (seção 1.0) — tabela agregada em Postgres."""
    return Dataset(
        name="vendas_agregado_uf",
        datasource=Datasource(
            type=DatasourceType.POSTGRES,
            connection_ref="env:DW_VENDAS_PG_URL",
        ),
        provides=Provides(
            dimensions={"sigla_uf"},
            measures={"valor_total", "quantidade"},
        ),
        model=TableModel(
            source="dw.vendas_agregado_uf",
            mapping={
                "sigla_uf": ColumnMapping(column="uf"),
                "valor_total": ColumnMapping(column="vl_total", agg=Aggregation.SUM),
                "quantidade": ColumnMapping(column="qt_total", agg=Aggregation.SUM),
            },
        ),
    )


def vendas_detalhado() -> Dataset:
    """Segundo dataset do schema `vendas` (seção 1.0) — star schema em Oracle."""
    return Dataset(
        name="vendas_detalhado",
        datasource=Datasource(
            type=DatasourceType.ORACLE,
            connection_ref="env:DW_VENDAS_ORACLE_URL",
        ),
        provides=Provides(
            dimensions={"sigla_uf", "cargo"},
            measures={"valor_total", "quantidade"},
        ),
        model=StarModel(
            fact=Fact(
                table="SCHEMA_DW.FT_VENDAS",
                mapping={
                    "valor_total": ColumnMapping(column="VL_TOTAL", agg=Aggregation.SUM),
                    "quantidade": ColumnMapping(column="QT_ITEM", agg=Aggregation.SUM),
                },
                keys={
                    "cliente_id": FactKey(column="CD_CLIENTE", references="dim_cliente.id"),
                    "cargo_id": FactKey(column="CD_CARGO", references="dim_cargo.id"),
                },
            ),
            dimension_tables={
                "dim_cliente": DimensionTable(
                    table="SCHEMA_DW.DM_CLIENTE",
                    primary_key="CD_CLIENTE",
                    mapping={"sigla_uf": ColumnMapping(column="SG_UF")},
                ),
                "dim_cargo": DimensionTable(
                    table="SCHEMA_DW.DM_CARGO",
                    primary_key="CD_CARGO",
                    mapping={"cargo": ColumnMapping(column="DS_CARGO")},
                ),
            },
            joins=(
                Join(from_ref="fato_vendas.cliente_id", to_ref="dim_cliente.id"),
                Join(from_ref="fato_vendas.cargo_id", to_ref="dim_cargo.id"),
            ),
        ),
    )


def vendas_schema() -> Schema:
    """Schema `vendas` da seção 1.0, com os dois datasets na ordem do YAML."""
    return Schema(
        name="vendas",
        version=1,
        description="Vendas — dataset escolhido automaticamente por cobertura de campos",
        dimensions={
            "sigla_uf": Dimension(name="sigla_uf", type=DataType.STRING, filterable=True),
            "cargo": Dimension(name="cargo", type=DataType.STRING, filterable=True),
        },
        measures={
            "valor_total": Measure(
                name="valor_total", agg=Aggregation.SUM, format="currency"
            ),
            "quantidade": Measure(name="quantidade", agg=Aggregation.SUM),
        },
        access_control=AccessControl(roles={"financeiro": {"valor_total", "quantidade"}}),
        datasets=(vendas_agregado_uf(), vendas_detalhado()),
    )


def vendas_schema_com_canal() -> Schema:
    """O schema `vendas` com a dimensão `canal` no modelo lógico, mas sem nenhum
    dataset que a proveja — reproduz o exemplo de erro da seção 2.5 ("...combinação de
    campos: sigla_uf, cargo, canal"). Só assim o erro é `no_dataset_available` e não
    `unknown_field`: `canal` existe no modelo lógico, só não é atendida fisicamente."""
    base = vendas_schema()
    return Schema(
        name=base.name,
        version=base.version,
        description=base.description,
        dimensions={
            **base.dimensions,
            "canal": Dimension(name="canal", type=DataType.STRING, filterable=True),
        },
        measures=base.measures,
        access_control=base.access_control,
        datasets=base.datasets,
    )


def eventos_navegacao_es() -> Dataset:
    """Dataset Elasticsearch da seção 1.1 — índice único, sem joins."""
    return Dataset(
        name="eventos_navegacao_es",
        datasource=Datasource(
            type=DatasourceType.ELASTICSEARCH,
            connection_ref="env:ES_EVENTOS_URL",
        ),
        provides=Provides(
            dimensions={"pais", "dispositivo"},
            measures={"duracao_media", "total_eventos"},
        ),
        model=IndexModel(
            name="eventos-navegacao",
            mapping={
                "pais": FieldMapping(field="pais", es_type="keyword"),
                "dispositivo": FieldMapping(field="dispositivo", es_type="keyword"),
                "duracao_media": FieldMapping(
                    field="duracao_sessao", agg=Aggregation.AVG
                ),
                "total_eventos": FieldMapping(
                    field="duracao_sessao", agg=Aggregation.VALUE_COUNT
                ),
            },
        ),
    )


def eventos_schema() -> Schema:
    """Modelo lógico que o dataset Elasticsearch da seção 1.1 atende."""
    return Schema(
        name="eventos_navegacao",
        version=1,
        dimensions={
            "pais": Dimension(name="pais"),
            "dispositivo": Dimension(name="dispositivo"),
        },
        measures={
            "duracao_media": Measure(name="duracao_media", agg=Aggregation.AVG),
            "total_eventos": Measure(name="total_eventos", agg=Aggregation.VALUE_COUNT),
        },
        datasets=(eventos_navegacao_es(),),
    )


def estoque_atual_pg() -> Dataset:
    """Dataset em modelo plano da seção 1.2 — uma visão, sem fato/dimensão."""
    return Dataset(
        name="estoque_atual_pg",
        datasource=Datasource(
            type=DatasourceType.POSTGRES,
            connection_ref="env:APP_ESTOQUE_URL",
        ),
        provides=Provides(
            dimensions={"filial", "produto"},
            measures={"quantidade_disponivel", "valor_unitario"},
        ),
        model=TableModel(
            source="app.vw_estoque_atual",
            mapping={
                "filial": ColumnMapping(column="filial"),
                "produto": ColumnMapping(column="produto"),
                "quantidade_disponivel": ColumnMapping(
                    column="qtd_disponivel", agg=Aggregation.SUM
                ),
                "valor_unitario": ColumnMapping(column="vl_unitario", agg=Aggregation.AVG),
            },
        ),
    )


def estoque_schema() -> Schema:
    """Modelo lógico que o dataset plano da seção 1.2 atende."""
    return Schema(
        name="estoque",
        version=1,
        dimensions={
            "filial": Dimension(name="filial"),
            "produto": Dimension(name="produto"),
        },
        measures={
            "quantidade_disponivel": Measure(
                name="quantidade_disponivel", agg=Aggregation.SUM
            ),
            "valor_unitario": Measure(name="valor_unitario", agg=Aggregation.AVG),
        },
        datasets=(estoque_atual_pg(),),
    )


def catalog() -> Catalog:
    """Os três schemas dos exemplos do documento, num único `Catalog` em memória."""
    return Catalog(
        schemas={
            "vendas": vendas_schema(),
            "eventos_navegacao": eventos_schema(),
            "estoque": estoque_schema(),
        }
    )
