"""Leitura de env vars e resolução de `connection_ref` — o único lugar do sistema que
lê `os.environ` diretamente (o resto recebe configuração já resolvida).
"""

import os
from dataclasses import dataclass


class ConfigError(Exception):
    """Variável de ambiente obrigatória ausente ou `connection_ref` malformado."""


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"Variável de ambiente obrigatória ausente: {name}.")
    return value


def resolve_connection_ref(connection_ref: str) -> str:
    """`env:DW_VENDAS_PG_URL` → o valor de verdade da env var `DW_VENDAS_PG_URL`.

    É o único formato de `connection_ref` que o catálogo usa, desde o primeiro
    exemplo da seção 1.0 — nenhum outro prefixo é suportado.
    """
    prefix = "env:"
    if not connection_ref.startswith(prefix):
        raise ConfigError(
            f"connection_ref '{connection_ref}' não começa com o prefixo '{prefix}' esperado."
        )
    return _require_env(connection_ref[len(prefix) :])


@dataclass(frozen=True, slots=True)
class Settings:
    """Configuração do composition root — lida uma vez, no boot do processo."""

    catalog_db_url: str
    redis_url: str
    internal_token: str
    git_sha: str = "unknown"
    light_timeout_seconds: float = 5.0
    heavy_timeout_seconds: float = 300.0
    cost_threshold: float = 10_000.0
    cache_ttl_seconds: int = 3600
    light_pool_size: int = 20
    heavy_pool_size: int = 3
    # --- Export de consultas pesadas (seção 2.4a) -------------------------------------
    #: Diretório onde o worker grava os CSV e de onde a API os serve. **Os dois
    #: processos precisam enxergar o mesmo caminho** — mesmo host, ou volume
    #: compartilhado (é a limitação do adapter de filesystem, ver
    #: `adapters/exports/local_file_exporter.py`).
    export_dir: str = "exports"
    export_ttl_seconds: int = 86_400
    # --- Observabilidade (Marco 9) ---------------------------------------------------
    slow_query_threshold_ms: int = 2000
    request_rate_limit: int = 100
    request_rate_limit_window_seconds: int = 60
    heavy_query_rate_limit: int = 5
    heavy_query_rate_limit_window_seconds: int = 60
    max_heavy_queue_depth: int = 100
    log_level: str = "INFO"


def load_settings() -> Settings:
    return Settings(
        catalog_db_url=_require_env("CATALOG_DB_URL"),
        redis_url=_require_env("REDIS_URL"),
        internal_token=_require_env("INTERNAL_TOKEN"),
        git_sha=os.environ.get("GIT_SHA", "unknown"),
        light_timeout_seconds=float(os.environ.get("LIGHT_TIMEOUT_SECONDS", "5.0")),
        heavy_timeout_seconds=float(os.environ.get("HEAVY_TIMEOUT_SECONDS", "300.0")),
        cost_threshold=float(os.environ.get("COST_THRESHOLD", "10000.0")),
        cache_ttl_seconds=int(os.environ.get("CACHE_TTL_SECONDS", "3600")),
        light_pool_size=int(os.environ.get("LIGHT_POOL_SIZE", "20")),
        heavy_pool_size=int(os.environ.get("HEAVY_POOL_SIZE", "3")),
        export_dir=os.environ.get("EXPORT_DIR", "exports"),
        export_ttl_seconds=int(os.environ.get("EXPORT_TTL_SECONDS", "86400")),
        slow_query_threshold_ms=int(os.environ.get("SLOW_QUERY_THRESHOLD_MS", "2000")),
        request_rate_limit=int(os.environ.get("REQUEST_RATE_LIMIT", "100")),
        request_rate_limit_window_seconds=int(
            os.environ.get("REQUEST_RATE_LIMIT_WINDOW_SECONDS", "60")
        ),
        heavy_query_rate_limit=int(os.environ.get("HEAVY_QUERY_RATE_LIMIT", "5")),
        heavy_query_rate_limit_window_seconds=int(
            os.environ.get("HEAVY_QUERY_RATE_LIMIT_WINDOW_SECONDS", "60")
        ),
        max_heavy_queue_depth=int(os.environ.get("MAX_HEAVY_QUEUE_DEPTH", "100")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
    )
