"""`LocalFileResultExporter` — export em disco de verdade, com `tmp_path`.

Sem Docker: filesystem é a infraestrutura aqui, e `tmp_path` já é filesystem real.
"""

import os
from datetime import UTC, datetime, timedelta

import pytest

from adapters.exports.local_file_exporter import (
    InvalidQueryIdError,
    LocalFileResultExporter,
)
from domain.models import Column, DataType, QueryResult

CSV_ESPERADO = "sigla_uf,valor_total\r\nSP,458320.5\r\nRJ,212904.1\r\n"


def _result(query_id: str = "q_8f2a1c") -> QueryResult:
    return QueryResult.completed(
        query_id=query_id,
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER, format="currency"),
        ),
        rows=(("SP", 458320.50), ("RJ", 212904.10)),
        dataset_used="vendas_agregado_uf",
        execution_ms=1200,
    )


@pytest.fixture
def exporter(tmp_path) -> LocalFileResultExporter:
    return LocalFileResultExporter(tmp_path / "exports", ttl_seconds=3600)


async def _read(exporter: LocalFileResultExporter, query_id: str) -> str:
    chunks = [chunk async for chunk in await exporter.open(query_id)]
    return b"".join(chunks).decode("utf-8")


# --- escrita ------------------------------------------------------------------------------


async def test_export_grava_o_csv_e_devolve_metadados(exporter):
    metadata = await exporter.export(_result())

    assert metadata.query_id == "q_8f2a1c"
    assert metadata.size_bytes == len(CSV_ESPERADO.encode("utf-8"))
    assert metadata.expires_at == metadata.created_at + timedelta(seconds=3600)
    assert await _read(exporter, "q_8f2a1c") == CSV_ESPERADO


async def test_export_cria_o_diretorio_se_nao_existir(tmp_path):
    destino = tmp_path / "ainda" / "nao" / "existe"
    exporter = LocalFileResultExporter(destino)

    await exporter.export(_result())

    assert (destino / "q_8f2a1c.csv").is_file()


async def test_export_substitui_o_anterior_do_mesmo_query_id(exporter):
    await exporter.export(_result())
    menor = QueryResult.completed(
        query_id="q_8f2a1c",
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("MG",),),
        dataset_used="vendas_agregado_uf",
    )

    await exporter.export(menor)

    assert await _read(exporter, "q_8f2a1c") == "sigla_uf\r\nMG\r\n"


async def test_export_nao_deixa_temporario_para_tras(exporter, tmp_path):
    await exporter.export(_result())

    assert [p.name for p in (tmp_path / "exports").iterdir()] == ["q_8f2a1c.csv"]


async def test_export_recusa_resultado_nao_concluido(exporter):
    with pytest.raises(ValueError, match="completed"):
        await exporter.export(QueryResult.processing("q_8f2a1c"))


async def test_ttl_precisa_ser_positivo(tmp_path):
    with pytest.raises(ValueError, match="positivo"):
        LocalFileResultExporter(tmp_path, ttl_seconds=0)


# --- travessia de caminho -----------------------------------------------------------------


@pytest.mark.parametrize(
    "query_id",
    ["../../etc/passwd", "q_../../x", "/etc/passwd", "q_ZZZZZZ", "", "q_8f2a1c/../x"],
)
async def test_query_id_invalido_nunca_vira_caminho(exporter, query_id):
    """`stat` de um `query_id` malformado é ausência, não erro — e o `export` recusa."""
    assert await exporter.stat(query_id) is None

    resultado = QueryResult.completed(
        query_id=query_id or "q_000000",
        columns=(Column(field="a", type=DataType.STRING),),
        rows=(("x",),),
        dataset_used="d",
    )
    if query_id:
        object.__setattr__(resultado, "query_id", query_id)
        with pytest.raises(InvalidQueryIdError):
            await exporter.export(resultado)


# --- leitura e TTL ------------------------------------------------------------------------


async def test_stat_de_export_inexistente_e_none(exporter):
    assert await exporter.stat("q_000000") is None


async def test_stat_de_export_vencido_e_none(tmp_path):
    """O TTL é autoritativo na leitura: vencido some da resposta antes de a varredura
    passar por ele."""
    exporter = LocalFileResultExporter(tmp_path, ttl_seconds=3600)
    await exporter.export(_result())
    _envelhecer(tmp_path / "q_8f2a1c.csv", horas=2)

    assert await exporter.stat("q_8f2a1c") is None
    assert (tmp_path / "q_8f2a1c.csv").is_file()  # ainda no disco, só não é servido


async def test_open_de_export_inexistente_levanta(exporter):
    with pytest.raises(FileNotFoundError):
        await exporter.open("q_000000")


async def test_open_sobrevive_a_remocao_durante_a_leitura(exporter, tmp_path):
    """O descritor é aberto antes do gerador: uma varredura concorrente não trunca um
    download em andamento."""
    await exporter.export(_result())
    stream = await exporter.open("q_8f2a1c")

    (tmp_path / "exports" / "q_8f2a1c.csv").unlink()

    conteudo = b"".join([chunk async for chunk in stream]).decode("utf-8")
    assert conteudo == CSV_ESPERADO


async def test_leitura_em_blocos_de_arquivo_grande(tmp_path):
    """Mais de um bloco de 64 KiB — o caminho que existe para não materializar."""
    exporter = LocalFileResultExporter(tmp_path)
    grande = QueryResult.completed(
        query_id="q_abcdef",
        columns=(Column(field="valor", type=DataType.STRING),),
        rows=tuple((f"linha-{i:06d}",) for i in range(20_000)),
        dataset_used="vendas_detalhado",
    )

    metadata = await exporter.export(grande)
    conteudo = await _read(exporter, "q_abcdef")

    assert metadata.size_bytes > 64 * 1024
    assert conteudo.startswith("valor\r\nlinha-000000\r\n")
    assert conteudo.endswith("linha-019999\r\n")


# --- varredura ----------------------------------------------------------------------------


async def test_purge_remove_so_os_vencidos(tmp_path):
    exporter = LocalFileResultExporter(tmp_path, ttl_seconds=3600)
    await exporter.export(_result("q_aaaaaa"))
    await exporter.export(_result("q_bbbbbb"))
    _envelhecer(tmp_path / "q_aaaaaa.csv", horas=2)

    removidos = await exporter.purge_expired()

    assert removidos == 1
    assert not (tmp_path / "q_aaaaaa.csv").exists()
    assert (tmp_path / "q_bbbbbb.csv").is_file()


async def test_purge_e_idempotente(tmp_path):
    exporter = LocalFileResultExporter(tmp_path, ttl_seconds=3600)
    await exporter.export(_result())
    _envelhecer(tmp_path / "q_8f2a1c.csv", horas=2)

    assert await exporter.purge_expired() == 1
    assert await exporter.purge_expired() == 0


async def test_purge_em_diretorio_inexistente_nao_falha(tmp_path):
    exporter = LocalFileResultExporter(tmp_path / "nunca-criado")

    assert await exporter.purge_expired() == 0


def _envelhecer(path, *, horas: int) -> None:
    """Recua o `mtime` do arquivo — é o que o exportador usa como data de criação."""
    quando = (datetime.now(UTC) - timedelta(hours=horas)).timestamp()
    os.utime(path, (quando, quando))
