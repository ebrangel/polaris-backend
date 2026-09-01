"""`infrastructure/config.py` — leitura de env vars e resolução de `connection_ref`.

Único lugar do sistema que lê `os.environ` diretamente; os testes usam
`monkeypatch.setenv`/`delenv` para isolar cada caso sem tocar no ambiente real.
"""

import pytest

from infrastructure.config import ConfigError, load_settings, resolve_connection_ref


# --- resolve_connection_ref ------------------------------------------------------------


def test_resolve_connection_ref_le_a_env_var_referenciada(monkeypatch):
    monkeypatch.setenv("DW_VENDAS_PG_URL", "postgresql+psycopg://user:pass@host/db")

    assert resolve_connection_ref("env:DW_VENDAS_PG_URL") == (
        "postgresql+psycopg://user:pass@host/db"
    )


def test_resolve_connection_ref_sem_prefixo_env_levanta_config_error():
    with pytest.raises(ConfigError, match="env:"):
        resolve_connection_ref("DW_VENDAS_PG_URL")


def test_resolve_connection_ref_com_env_var_ausente_levanta_config_error(monkeypatch):
    monkeypatch.delenv("VARIAVEL_QUE_NAO_EXISTE", raising=False)

    with pytest.raises(ConfigError, match="VARIAVEL_QUE_NAO_EXISTE"):
        resolve_connection_ref("env:VARIAVEL_QUE_NAO_EXISTE")


# --- load_settings ----------------------------------------------------------------------


@pytest.fixture
def env_obrigatorio(monkeypatch):
    monkeypatch.setenv("CATALOG_DB_URL", "postgresql+psycopg://user:pass@host/catalogo")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("INTERNAL_TOKEN", "token-secreto")


def test_load_settings_com_env_obrigatorio_preenchido(env_obrigatorio):
    settings = load_settings()

    assert settings.catalog_db_url == "postgresql+psycopg://user:pass@host/catalogo"
    assert settings.redis_url == "redis://localhost:6379/0"
    assert settings.internal_token == "token-secreto"
    assert settings.git_sha == "unknown"  # default sem GIT_SHA no ambiente


@pytest.mark.parametrize(
    "missing_var", ["CATALOG_DB_URL", "REDIS_URL", "INTERNAL_TOKEN"]
)
def test_load_settings_sem_env_obrigatoria_levanta_config_error(
    env_obrigatorio, monkeypatch, missing_var
):
    monkeypatch.delenv(missing_var, raising=False)

    with pytest.raises(ConfigError, match=missing_var):
        load_settings()


def test_load_settings_le_os_opcionais_quando_presentes(env_obrigatorio, monkeypatch):
    monkeypatch.setenv("GIT_SHA", "cafe1234")
    monkeypatch.setenv("QUERY_TIMEOUT_SECONDS", "600")
    monkeypatch.setenv("INLINE_WAIT_SECONDS", "1.5")
    monkeypatch.setenv("INLINE_WAIT_POLL_DELAY", "0.05")
    monkeypatch.setenv("CACHE_TTL_SECONDS", "60")
    monkeypatch.setenv("CACHE_MAX_ROWS", "5000")
    monkeypatch.setenv("CACHE_MAX_PAYLOAD_BYTES", "1048576")
    monkeypatch.setenv("DEFAULT_MAX_LIMIT", "20000")
    monkeypatch.setenv("QUERY_POOL_SIZE", "4")
    monkeypatch.setenv("SLOW_QUERY_THRESHOLD_MS", "500")
    monkeypatch.setenv("REQUEST_RATE_LIMIT", "50")
    monkeypatch.setenv("REQUEST_RATE_LIMIT_WINDOW_SECONDS", "30")
    monkeypatch.setenv("MAX_QUEUE_DEPTH", "10")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    settings = load_settings()

    assert settings.git_sha == "cafe1234"
    assert settings.query_timeout_seconds == 600.0
    assert settings.inline_wait_seconds == 1.5
    assert settings.inline_wait_poll_delay == 0.05
    assert settings.cache_ttl_seconds == 60
    assert settings.cache_max_rows == 5000
    assert settings.cache_max_payload_bytes == 1_048_576
    assert settings.default_max_limit == 20_000
    assert settings.query_pool_size == 4
    assert settings.slow_query_threshold_ms == 500
    assert settings.request_rate_limit == 50
    assert settings.request_rate_limit_window_seconds == 30
    assert settings.max_queue_depth == 10
    assert settings.log_level == "DEBUG"


def test_load_settings_usa_defaults_dos_opcionais_ausentes(env_obrigatorio):
    settings = load_settings()

    assert settings.query_timeout_seconds == 300.0
    assert settings.inline_wait_seconds == 2.0
    assert settings.inline_wait_poll_delay == 0.1
    assert settings.cache_ttl_seconds == 3600
    assert settings.cache_max_rows == 100_000
    assert settings.cache_max_payload_bytes == 8 * 1024 * 1024
    assert settings.default_max_limit == 50_000
    assert settings.query_pool_size == 10
    assert settings.slow_query_threshold_ms == 2000
    assert settings.request_rate_limit == 100
    assert settings.request_rate_limit_window_seconds == 60
    assert settings.max_queue_depth == 100
    assert settings.log_level == "INFO"
