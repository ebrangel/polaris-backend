"""Regra de dependência da Clean Architecture, verificada automaticamente.

`domain/` não pode importar nada de framework, infraestrutura nem de camadas externas.
`application/` pode importar `domain/`, mas não framework nem `adapters/`/`infrastructure/`.
Este teste vale para todos os marcos seguintes: qualquer import proibido quebra a suíte
antes de virar um acoplamento silencioso.
"""

import ast
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
DOMAIN_DIR = SRC_DIR / "domain"
APPLICATION_DIR = SRC_DIR / "application"

INFRASTRUCTURE_ROOTS = frozenset(
    {
        "fastapi",
        "starlette",
        "pydantic",
        "sqlalchemy",
        "redis",
        "celery",
        "elasticsearch",
        "psycopg",
        "oracledb",
        "yaml",
        "httpx",
        "requests",
    }
)

#: Camadas mais externas que cada camada não pode importar (regra de dependência do CLAUDE.md).
FORBIDDEN_BY_LAYER: dict[Path, frozenset[str]] = {
    DOMAIN_DIR: INFRASTRUCTURE_ROOTS | {"application", "adapters", "infrastructure"},
    APPLICATION_DIR: INFRASTRUCTURE_ROOTS | {"adapters", "infrastructure"},
}


def imported_roots(module_path: Path) -> set[str]:
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            roots.add(node.module.split(".", 1)[0])
    return roots


def _all_modules() -> list[Path]:
    modules = []
    for layer_dir in FORBIDDEN_BY_LAYER:
        modules.extend(sorted(layer_dir.rglob("*.py")))
    return modules


@pytest.mark.parametrize(
    "module_path", _all_modules(), ids=lambda p: str(p.relative_to(SRC_DIR))
)
def test_camada_nao_importa_framework_nem_camada_externa(module_path):
    for layer_dir, forbidden_roots in FORBIDDEN_BY_LAYER.items():
        if layer_dir in module_path.parents or layer_dir == module_path.parent:
            forbidden = sorted(imported_roots(module_path) & forbidden_roots)
            assert not forbidden, (
                f"{module_path.relative_to(SRC_DIR)} importa {', '.join(forbidden)}"
            )
            return
    pytest.fail(f"{module_path} não pertence a nenhuma camada mapeada")


def test_as_camadas_foram_encontradas():
    assert DOMAIN_DIR.is_dir()
    assert list(DOMAIN_DIR.glob("*.py"))
    assert APPLICATION_DIR.is_dir()
    assert list(APPLICATION_DIR.rglob("*.py"))
