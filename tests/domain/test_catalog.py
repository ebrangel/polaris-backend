"""Invariantes do catálogo, verificadas na construção das entidades."""

import dataclasses

import pytest
from fixtures import (
    catalog,
    estoque_atual_pg,
    estoque_schema,
    eventos_navegacao_es,
    eventos_schema,
    vendas_agregado_uf,
    vendas_detalhado,
    vendas_schema,
)

from domain.errors import InvalidCatalogError, UnknownFieldError, UnknownSchemaError
from domain.models import (
    AccessControl,
    Aggregation,
    Catalog,
    ColumnMapping,
    Dataset,
    Datasource,
    DatasourceType,
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


# --- Os três exemplos da documentação constroem sem erro ------------------------------


def test_exemplos_da_documentacao_sao_catalogos_validos():
    for schema in (vendas_schema(), eventos_schema(), estoque_schema()):
        assert schema.datasets


def test_ordem_dos_datasets_e_preservada():
    """A ordem de declaração é a política de otimização (seção 1.0)."""
    assert [d.name for d in vendas_schema().datasets] == [
        "vendas_agregado_uf",
        "vendas_detalhado",
    ]


# --- Regra 1: `provides` só cita campos do modelo lógico -------------------------------


def test_provides_com_dimensao_inexistente_no_modelo_logico():
    dataset = Dataset(
        name="com_canal",
        datasource=Datasource(
            type=DatasourceType.POSTGRES, connection_ref="env:DW_VENDAS_PG_URL"
        ),
        provides=Provides(dimensions={"sigla_uf", "canal"}, measures={"valor_total"}),
        model=TableModel(
            source="dw.vendas_agregado_uf",
            mapping={
                "sigla_uf": ColumnMapping(column="uf"),
                "canal": ColumnMapping(column="canal"),
                "valor_total": ColumnMapping(column="vl_total", agg=Aggregation.SUM),
            },
        ),
    )
    base = vendas_schema()

    with pytest.raises(InvalidCatalogError) as excinfo:
        Schema(
            name=base.name,
            version=base.version,
            dimensions=base.dimensions,
            measures=base.measures,
            datasets=(dataset,),
        )

    assert excinfo.value.fields == ("canal",)


def test_provides_com_medida_inexistente_no_modelo_logico():
    base = vendas_schema()
    dataset = Dataset(
        name="com_ticket_medio",
        datasource=Datasource(
            type=DatasourceType.POSTGRES, connection_ref="env:DW_VENDAS_PG_URL"
        ),
        provides=Provides(dimensions={"sigla_uf"}, measures={"ticket_medio"}),
        model=TableModel(
            source="dw.vendas_agregado_uf",
            mapping={
                "sigla_uf": ColumnMapping(column="uf"),
                "ticket_medio": ColumnMapping(column="vl_ticket", agg=Aggregation.AVG),
            },
        ),
    )

    with pytest.raises(InvalidCatalogError) as excinfo:
        Schema(
            name=base.name,
            version=base.version,
            dimensions=base.dimensions,
            measures=base.measures,
            datasets=(dataset,),
        )

    assert excinfo.value.fields == ("ticket_medio",)


# --- Regra 2: todo campo de `provides` tem mapeamento físico ---------------------------


def test_provides_sem_mapeamento_fisico_em_tabela():
    with pytest.raises(InvalidCatalogError) as excinfo:
        Dataset(
            name="vendas_agregado_uf",
            datasource=Datasource(
                type=DatasourceType.POSTGRES, connection_ref="env:DW_VENDAS_PG_URL"
            ),
            provides=Provides(dimensions={"sigla_uf"}, measures={"valor_total"}),
            model=TableModel(
                source="dw.vendas_agregado_uf",
                mapping={"sigla_uf": ColumnMapping(column="uf")},
            ),
        )

    assert excinfo.value.fields == ("valor_total",)


def test_provides_sem_mapeamento_fisico_em_star_schema():
    star = vendas_detalhado().model

    with pytest.raises(InvalidCatalogError) as excinfo:
        Dataset(
            name="vendas_detalhado",
            datasource=Datasource(
                type=DatasourceType.ORACLE, connection_ref="env:DW_VENDAS_ORACLE_URL"
            ),
            provides=Provides(
                dimensions={"sigla_uf", "cargo", "canal"},
                measures={"valor_total", "quantidade"},
            ),
            model=star,
        )

    assert excinfo.value.fields == ("canal",)


def test_star_schema_junta_o_mapeamento_do_fato_e_das_dimensoes():
    star = vendas_detalhado().model

    assert star.mapped_fields() == {"valor_total", "quantidade", "sigla_uf", "cargo"}


# --- Regra 3: Elasticsearch só suporta modelo plano ------------------------------------


def test_dataset_elasticsearch_nao_pode_ser_star_schema():
    with pytest.raises(InvalidCatalogError, match="só suporta modelo plano"):
        Dataset(
            name="eventos_star",
            datasource=Datasource(
                type=DatasourceType.ELASTICSEARCH, connection_ref="env:ES_EVENTOS_URL"
            ),
            provides=Provides(dimensions={"sigla_uf"}, measures={"valor_total"}),
            model=vendas_detalhado().model,
        )


def test_dataset_elasticsearch_nao_pode_ser_tabela_relacional():
    with pytest.raises(InvalidCatalogError, match="só suporta modelo plano"):
        Dataset(
            name="eventos_tabela",
            datasource=Datasource(
                type=DatasourceType.ELASTICSEARCH, connection_ref="env:ES_EVENTOS_URL"
            ),
            provides=Provides(dimensions={"pais"}),
            model=TableModel(
                source="app.eventos", mapping={"pais": ColumnMapping(column="pais")}
            ),
        )


def test_dataset_relacional_nao_pode_declarar_indice():
    with pytest.raises(InvalidCatalogError, match="declara um índice"):
        Dataset(
            name="estoque_indice",
            datasource=Datasource(
                type=DatasourceType.POSTGRES, connection_ref="env:APP_ESTOQUE_URL"
            ),
            provides=Provides(dimensions={"pais"}),
            model=IndexModel(
                name="eventos-navegacao",
                mapping={"pais": FieldMapping(field="pais", es_type="keyword")},
            ),
        )


# --- Regra 4: joins e chaves só apontam para dimensões declaradas ----------------------


def test_join_para_dimensao_nao_declarada():
    with pytest.raises(InvalidCatalogError, match="dim_produto"):
        StarModel(
            fact=Fact(
                table="SCHEMA_DW.FT_VENDAS",
                mapping={"valor_total": ColumnMapping(column="VL_TOTAL", agg=Aggregation.SUM)},
            ),
            dimension_tables={
                "dim_cliente": DimensionTable(
                    table="SCHEMA_DW.DM_CLIENTE",
                    primary_key="CD_CLIENTE",
                    mapping={"sigla_uf": ColumnMapping(column="SG_UF")},
                )
            },
            joins=(Join(from_ref="fato_vendas.produto_id", to_ref="dim_produto.id"),),
        )


def test_chave_do_fato_para_dimensao_nao_declarada():
    with pytest.raises(InvalidCatalogError, match="dim_cargo"):
        StarModel(
            fact=Fact(
                table="SCHEMA_DW.FT_VENDAS",
                mapping={"valor_total": ColumnMapping(column="VL_TOTAL", agg=Aggregation.SUM)},
                keys={"cargo_id": FactKey(column="CD_CARGO", references="dim_cargo.id")},
            ),
            dimension_tables={
                "dim_cliente": DimensionTable(
                    table="SCHEMA_DW.DM_CLIENTE",
                    primary_key="CD_CLIENTE",
                    mapping={"sigla_uf": ColumnMapping(column="SG_UF")},
                )
            },
        )


def test_a_chave_depois_do_ponto_nao_e_validada():
    """`references: dim_cliente.id` convive com `primary_key: CD_CLIENTE` na própria
    documentação — só o alias da dimensão é verificado."""
    star = vendas_detalhado().model

    assert star.fact.keys["cliente_id"].dimension_alias == "dim_cliente"
    assert star.dimension_tables["dim_cliente"].primary_key == "CD_CLIENTE"


# --- Regra 5: nomes de dataset únicos --------------------------------------------------


def test_datasets_com_nome_repetido():
    base = vendas_schema()

    with pytest.raises(InvalidCatalogError, match="mais de um dataset"):
        Schema(
            name=base.name,
            version=base.version,
            dimensions=base.dimensions,
            measures=base.measures,
            datasets=(vendas_agregado_uf(), vendas_agregado_uf()),
        )


# --- Regra 6: access_control só cita medidas existentes --------------------------------


def test_access_control_com_medida_inexistente():
    base = vendas_schema()

    with pytest.raises(InvalidCatalogError) as excinfo:
        Schema(
            name=base.name,
            version=base.version,
            dimensions=base.dimensions,
            measures=base.measures,
            datasets=base.datasets,
            access_control=AccessControl(roles={"financeiro": {"margem_bruta"}}),
        )

    assert excinfo.value.fields == ("margem_bruta",)


def test_access_control_agrega_medidas_de_varios_roles():
    control = AccessControl(
        roles={"financeiro": {"valor_total"}, "operacoes": {"quantidade"}}
    )

    assert control.allowed_measures(["financeiro"]) == {"valor_total"}
    assert control.allowed_measures(["financeiro", "operacoes"]) == {
        "valor_total",
        "quantidade",
    }
    assert control.allowed_measures(["desconhecido"]) == frozenset()


# --- Consistência do modelo lógico -----------------------------------------------------


def test_dimensao_indexada_com_nome_divergente():
    with pytest.raises(InvalidCatalogError, match="indexada como"):
        Schema(
            name="vendas",
            version=1,
            dimensions={"uf": Dimension(name="sigla_uf")},
            measures={},
        )


def test_nome_nao_pode_ser_dimensao_e_medida_ao_mesmo_tempo():
    with pytest.raises(InvalidCatalogError, match="ao mesmo tempo"):
        Schema(
            name="vendas",
            version=1,
            dimensions={"valor_total": Dimension(name="valor_total")},
            measures={"valor_total": Measure(name="valor_total", agg=Aggregation.SUM)},
        )


# --- Tradução lógico → físico ----------------------------------------------------------


def test_traducao_de_nome_logico_em_tabela_unica():
    dataset = estoque_atual_pg()

    assert dataset.physical_for("filial") == ColumnMapping(column="filial")
    assert dataset.physical_for("valor_unitario") == ColumnMapping(
        column="vl_unitario", agg=Aggregation.AVG
    )


def test_traducao_de_nome_logico_em_star_schema():
    dataset = vendas_detalhado()

    assert dataset.physical_for("valor_total") == ColumnMapping(
        column="VL_TOTAL", agg=Aggregation.SUM
    )
    assert dataset.physical_for("sigla_uf") == ColumnMapping(column="SG_UF")
    assert dataset.physical_for("cargo") == ColumnMapping(column="DS_CARGO")


def test_traducao_de_nome_logico_em_elasticsearch():
    dataset = eventos_navegacao_es()

    assert dataset.physical_for("pais") == FieldMapping(field="pais", es_type="keyword")
    assert dataset.physical_for("total_eventos") == FieldMapping(
        field="duracao_sessao", agg=Aggregation.VALUE_COUNT
    )


def test_traducao_de_campo_nao_mapeado():
    with pytest.raises(UnknownFieldError) as excinfo:
        vendas_agregado_uf().physical_for("cargo")

    assert excinfo.value.fields == ("cargo",)


# --- Imutabilidade ---------------------------------------------------------------------


def test_entidades_do_catalogo_sao_imutaveis():
    schema = vendas_schema()

    with pytest.raises(dataclasses.FrozenInstanceError):
        schema.name = "outro"

    with pytest.raises(TypeError):
        schema.dimensions["novo"] = Dimension(name="novo")


# --- Agregado Catalog (nome → Schema, em memória) ---------------------------------------


def test_catalog_resolve_schema_pelo_nome():
    cat = catalog()

    assert cat.get_schema("vendas").name == "vendas"
    assert cat.get_schema("estoque").name == "estoque"


def test_catalog_schema_desconhecido_e_unknown_schema_error():
    cat = catalog()

    with pytest.raises(UnknownSchemaError) as excinfo:
        cat.get_schema("inexistente")

    assert excinfo.value.fields == ("inexistente",)


def test_catalog_schema_names_preserva_ordem_de_insercao():
    cat = catalog()

    assert cat.schema_names() == ("vendas", "eventos_navegacao", "estoque")


def test_catalog_contains():
    cat = catalog()

    assert "vendas" in cat
    assert "inexistente" not in cat


def test_catalog_rejeita_chave_divergente_do_nome_do_schema():
    with pytest.raises(InvalidCatalogError, match="indexa"):
        Catalog(schemas={"outro_nome": vendas_schema()})
