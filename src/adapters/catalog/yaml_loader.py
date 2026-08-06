"""Leitura dos YAML de `catalog/schemas/` — PyYAML entra só aqui.

O resto do sistema trabalha com o `dict` resultante de `yaml.safe_load`
(`application/catalog_codec.py`) ou com o `Schema` já compilado — nunca com este
módulo diretamente, fora do script de publicação (Marco 8) e dos testes.
"""

from pathlib import Path
from typing import Any

import yaml

#: `catalog/schemas/` na raiz do repositório — onde o pipeline de publicação
#: (`docs/pipeline-publicacao.md`: "catalog/schemas/*.yaml") espera os arquivos.
DEFAULT_SCHEMAS_DIR = Path(__file__).resolve().parents[3] / "catalog" / "schemas"


def load_schema_file(path: Path) -> dict[str, Any]:
    """Um arquivo YAML → `dict`, pronto para `catalog_codec.schema_from_dict`."""
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def list_schema_files(schemas_dir: Path = DEFAULT_SCHEMAS_DIR) -> tuple[Path, ...]:
    """`catalog/schemas/*.yaml`, em ordem alfabética — execução determinística no CI."""
    return tuple(sorted(schemas_dir.glob("*.yaml")))
