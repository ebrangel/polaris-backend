"""O teto do sink de cache — sem Docker, porque o que se testa aqui é o comportamento de
memória do adapter, não o Redis.

O ponto do Marco 12 é que o pico de memória do worker deixa de crescer com o resultado.
Para o cache isso quer dizer: ao passar do teto, o sink **descarta o que acumulou** e
para de acumular, em vez de seguir até o fim para só então descobrir que não cabe.
"""

import logging

from adapters.cache.redis_cache import RedisCacheGateway
from application.ports.row_sink import StreamedResult
from domain.models import Column, DataType

COLUNAS = (
    Column(field="sigla_uf", type=DataType.STRING),
    Column(field="valor_total", type=DataType.NUMBER),
)


class _FakeRedis:
    """Só o que o sink usa: `set`. Guarda o que foi gravado, para inspeção."""

    def __init__(self) -> None:
        self.stored: dict[str, tuple[str, int | None]] = {}

    async def set(self, key, value, ex=None):
        self.stored[key] = (value, ex)


def _cache(client, **kwargs) -> RedisCacheGateway:
    return RedisCacheGateway(client, key_prefix="q:", **kwargs)


async def _sink(cache, **kwargs):
    return await cache.open_writer("vendas:q_1", COLUNAS, "q_1", "vendas_agregado_uf", **kwargs)


RESULTADO = StreamedResult(row_count=2, total_rows=7, execution_ms=12)


async def test_grava_o_documento_da_secao_2_3_no_close():
    client = _FakeRedis()
    sink = await _sink(_cache(client))

    await sink.write([("SP", 458320.50)])
    await sink.write([("RJ", 212904.10)])
    await sink.close(RESULTADO)

    import json

    payload, ttl = client.stored["q:vendas:q_1"]
    documento = json.loads(payload)
    assert documento["query_id"] == "q_1"
    assert documento["status"] == "completed"
    assert documento["rows"] == [["SP", 458320.5], ["RJ", 212904.1]]
    assert documento["meta"]["total_rows"] == 7
    assert documento["meta"]["cached"] is False
    assert [c["field"] for c in documento["columns"]] == ["sigla_uf", "valor_total"]
    assert ttl == 3600


async def test_o_documento_gravado_volta_pelo_get(monkeypatch):
    """Round-trip real: o documento montado por concatenação tem de ser exatamente o que
    `dict_to_result` sabe ler — é o risco de montar JSON à mão em vez de `json.dumps`."""
    client = _FakeRedis()
    cache = _cache(client)
    sink = await _sink(cache)
    await sink.write([("SP", 458320.50), ("RJ", 212904.10)])
    await sink.close(RESULTADO)

    gravado, _ = client.stored["q:vendas:q_1"]

    async def _get(key):
        return gravado

    async def _incr(key):
        return 1

    monkeypatch.setattr(client, "get", _get, raising=False)
    monkeypatch.setattr(client, "incr", _incr, raising=False)

    result = await cache.get("vendas:q_1")

    assert result.rows == (("SP", 458320.5), ("RJ", 212904.1))
    assert result.meta.total_rows == 7
    assert result.meta.row_count == 2
    assert result.columns == COLUNAS


async def test_desiste_no_teto_de_linhas_e_nao_grava(caplog):
    client = _FakeRedis()
    sink = await _sink(_cache(client, max_rows=2))

    with caplog.at_level(logging.INFO):
        await sink.write([("SP", 1.0), ("RJ", 2.0)])
        await sink.write([("MG", 3.0)])
        await sink.close(StreamedResult(row_count=3, total_rows=3, execution_ms=1))

    assert client.stored == {}
    assert "não cacheado" in caplog.text


async def test_no_teto_exato_ainda_cabe():
    """O corte é **acima** do teto, não a partir dele."""
    client = _FakeRedis()
    sink = await _sink(_cache(client, max_rows=2))

    await sink.write([("SP", 1.0), ("RJ", 2.0)])
    await sink.close(RESULTADO)

    assert "q:vendas:q_1" in client.stored


async def test_desiste_no_teto_de_bytes_e_libera_o_buffer():
    """Passar do teto não é só "não gravar": é parar de acumular. Sem isso o worker
    carregaria na memória um resultado inteiro que sabidamente vai jogar fora."""
    client = _FakeRedis()
    sink = await _sink(_cache(client, max_rows=None, max_payload_bytes=60))

    for i in range(500):
        await sink.write([(f"UF{i}", float(i))])

    assert sink._rows == []  # buffer liberado, e não crescendo até o fim
    await sink.close(StreamedResult(row_count=500, total_rows=500, execution_ms=1))
    assert client.stored == {}


async def test_tetos_desligados_gravam_qualquer_tamanho():
    client = _FakeRedis()
    sink = await _sink(_cache(client, max_rows=None, max_payload_bytes=None))

    for i in range(500):
        await sink.write([(f"UF{i}", float(i))])
    await sink.close(StreamedResult(row_count=500, total_rows=500, execution_ms=1))

    payload, _ = client.stored["q:vendas:q_1"]
    assert payload.count("[") > 500


async def test_ttl_explicito_vence_o_padrao():
    client = _FakeRedis()
    sink = await _sink(_cache(client, default_ttl_seconds=3600), ttl_seconds=60)

    await sink.write([("SP", 1.0)])
    await sink.close(StreamedResult(row_count=1, total_rows=1, execution_ms=1))

    _, ttl = client.stored["q:vendas:q_1"]
    assert ttl == 60


async def test_resultado_vazio_grava_uma_entrada_valida():
    """Zero linhas é um resultado legítimo, e cacheá-lo evita re-executar uma consulta
    cara que não devolve nada."""
    import json

    client = _FakeRedis()
    sink = await _sink(_cache(client))

    await sink.close(StreamedResult(row_count=0, total_rows=0, execution_ms=5))

    payload, _ = client.stored["q:vendas:q_1"]
    assert json.loads(payload)["rows"] == []
