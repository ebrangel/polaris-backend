"""`catalog_codec` — o codec `dict` ↔ `Schema` (Marco 8).

Amarra os três YAMLs reais de `catalog/schemas/` aos fixtures de
`tests/fixtures.py`: carregar cada arquivo tem que produzir exatamente o mesmo
`Schema` que os testes anteriores já validam à mão.
"""

import json

import pytest
import yaml
from fixtures import estoque_schema, eventos_schema, vendas_schema

from adapters.catalog.yaml_loader import DEFAULT_SCHEMAS_DIR, load_schema_file
from application.catalog_codec import (
    canonical_json,
    compile_schema,
    content_hash,
    decompile_schema,
    schema_from_dict,
    schema_to_dict,
)
from domain.errors import InvalidCatalogError


# --- Round-trip contra os YAMLs reais -----------------------------------------------------


def test_vendas_yaml_produz_o_schema_do_fixture():
    data = load_schema_file(DEFAULT_SCHEMAS_DIR / "vendas.yaml")

    assert schema_from_dict(data) == vendas_schema()


def test_eventos_navegacao_yaml_produz_o_schema_do_fixture():
    data = load_schema_file(DEFAULT_SCHEMAS_DIR / "eventos_navegacao.yaml")

    assert schema_from_dict(data) == eventos_schema()


def test_estoque_yaml_produz_o_schema_do_fixture():
    data = load_schema_file(DEFAULT_SCHEMAS_DIR / "estoque.yaml")

    assert schema_from_dict(data) == estoque_schema()


@pytest.mark.parametrize(
    "schema_factory", [vendas_schema, eventos_schema, estoque_schema], ids=lambda f: f.__name__
)
def test_schema_to_dict_depois_schema_from_dict_e_o_mesmo_schema(schema_factory):
    schema = schema_factory()

    assert schema_from_dict(schema_to_dict(schema)) == schema


# --- JSON canônico e hash ------------------------------------------------------------------


def test_canonical_json_ordena_as_chaves():
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})


def test_content_hash_e_sha256_hexdigest_estavel():
    content = canonical_json({"a": 1})

    assert content_hash(content) == content_hash(content)
    assert len(content_hash(content)) == 64


def test_compile_schema_hash_e_estavel_entre_execucoes():
    data = load_schema_file(DEFAULT_SCHEMAS_DIR / "vendas.yaml")

    _, _, hash_a = compile_schema(data)
    _, _, hash_b = compile_schema(data)

    assert hash_a == hash_b


def test_reordenar_chaves_do_yaml_nao_muda_o_hash():
    """O hash é do **compilado**, não do YAML cru — reordenar chaves no arquivo (ou
    equivalente: montar o mesmo dict em ordem diferente) não pode disparar uma
    republicação (`docs/pipeline-publicacao.md`)."""
    original = load_schema_file(DEFAULT_SCHEMAS_DIR / "estoque.yaml")
    reordenado = json.loads(json.dumps(original))  # dict comum, mesma ordem de chaves
    reordenado["measures"] = dict(reversed(list(reordenado["measures"].items())))

    _, _, hash_original = compile_schema(original)
    _, _, hash_reordenado = compile_schema(reordenado)

    assert hash_original == hash_reordenado


def test_decompile_schema_e_o_inverso_de_compile_schema():
    data = load_schema_file(DEFAULT_SCHEMAS_DIR / "vendas.yaml")

    schema, content, _ = compile_schema(data)

    assert decompile_schema(content) == schema


# --- Erros de forma -------------------------------------------------------------------------


def test_schema_from_dict_sem_campo_obrigatorio_levanta_invalid_catalog_error():
    data = load_schema_file(DEFAULT_SCHEMAS_DIR / "vendas.yaml")
    del data["schema"]

    with pytest.raises(InvalidCatalogError):
        schema_from_dict(data)


def test_schema_from_dict_com_agg_invalida_levanta_invalid_catalog_error():
    data = load_schema_file(DEFAULT_SCHEMAS_DIR / "vendas.yaml")
    data["measures"]["valor_total"]["agg"] = "nao_existe"

    with pytest.raises(InvalidCatalogError):
        schema_from_dict(data)


def test_dataset_sem_table_index_ou_fact_levanta_invalid_catalog_error():
    data = load_schema_file(DEFAULT_SCHEMAS_DIR / "estoque.yaml")
    del data["datasets"][0]["table"]

    with pytest.raises(InvalidCatalogError):
        schema_from_dict(data)


def test_yaml_bruto_carrega_para_o_dict_esperado_pelo_codec():
    """Só para deixar explícito o contrato do `yaml_loader`: `yaml.safe_load` de um
    dos arquivos reais já produz o formato que `schema_from_dict` espera, sem
    tradução extra nenhuma."""
    with (DEFAULT_SCHEMAS_DIR / "eventos_navegacao.yaml").open() as stream:
        data = yaml.safe_load(stream)

    assert schema_from_dict(data) == eventos_schema()
