"""Leitura de env vars e resolução de `connection_ref` — o único lugar do sistema que
lê `os.environ` diretamente (o resto recebe configuração já resolvida).
"""

import os
from dataclasses import dataclass

from dotenv import load_dotenv


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
    #: Limiar da heurística de custo (contagem de campos), usada quando o dialeto não
    #: tem `EXPLAIN` coberto ou ele falha. Escala diferente de `cost_threshold` — ver
    #: `SQLAlchemyQueryExecutor.__init__`.
    heuristic_cost_threshold: float = 30.0
    cache_ttl_seconds: int = 3600
    #: Tetos do que vale a pena guardar no Redis: acima deles o resultado não é
    #: cacheado (a consulta seguinte executa de novo, e nada falha).
    cache_max_rows: int = 100_000
    cache_max_payload_bytes: int = 8 * 1024 * 1024
    #: Teto de linhas para schema que não declara `max_limit` no catálogo — sem ele uma
    #: consulta sem `limit` vira `SELECT` sem `LIMIT`, e o resultado inteiro é
    #: materializado em memória.
    default_max_limit: int = 50_000
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
    # `override=False` (padrão): variáveis já presentes no ambiente real vencem o
    # `.env` — em produção/CI a orquestração define as env vars diretamente, e o
    # arquivo não deve pisar nelas. Sem `.env` no working directory (caso normal em
    # produção), a chamada é um no-op.
    load_dotenv()
    return Settings(
        catalog_db_url=_require_env("CATALOG_DB_URL"),
        redis_url=_require_env("REDIS_URL"),
        internal_token=_require_env("INTERNAL_TOKEN"),
        git_sha=os.environ.get("GIT_SHA", "unknown"),
        light_timeout_seconds=float(os.environ.get("LIGHT_TIMEOUT_SECONDS", "5.0")),
        heavy_timeout_seconds=float(os.environ.get("HEAVY_TIMEOUT_SECONDS", "300.0")),
        cost_threshold=float(os.environ.get("COST_THRESHOLD", "10000.0")),
        heuristic_cost_threshold=float(
            os.environ.get("HEURISTIC_COST_THRESHOLD", "30.0")
        ),
        cache_ttl_seconds=int(os.environ.get("CACHE_TTL_SECONDS", "3600")),
        cache_max_rows=int(os.environ.get("CACHE_MAX_ROWS", "100000")),
        cache_max_payload_bytes=int(
            os.environ.get("CACHE_MAX_PAYLOAD_BYTES", str(8 * 1024 * 1024))
        ),
        default_max_limit=int(os.environ.get("DEFAULT_MAX_LIMIT", "50000")),
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
