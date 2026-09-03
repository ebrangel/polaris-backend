"""`LocalFileResultExporter` — export em disco de verdade, com `tmp_path`.

Sem Docker: filesystem é a infraestrutura aqui, e `tmp_path` já é filesystem real.
"""

import json
import os
from datetime import UTC, datetime, timedelta

import pytest

from adapters.exports.local_file_exporter import (
    InvalidQueryIdError,
    LocalFileResultExporter,
)
from application.ports.result_exporter import ExportKind
from application.ports.row_sink import StreamedResult
from domain.models import Column, DataType

CSV_ESPERADO = "sigla_uf,valor_total\r\nSP,458320.5\r\nRJ,212904.1\r\n"
JSONL_ESPERADO = '["SP", 458320.5]\n["RJ", 212904.1]\n'

COLUNAS = (
    Column(field="sigla_uf", type=DataType.STRING),
    Column(field="valor_total", type=DataType.NUMBER, format="currency"),
)
LINHAS = (("SP", 458320.50), ("RJ", 212904.10))


async def _export(
    exporter: LocalFileResultExporter,
    query_id: str = "q_8f2a1c",
    *,
    columns=COLUNAS,
    rows=LINHAS,
    chunk_size: int = 1,
    total_rows: int | None = None,
    dataset_used: str = "vendas_agregado_uf",
    execution_ms: int = 1200,
):
    """Encena o worker: abre o sink, empurra as linhas em blocos e fecha.

    `chunk_size=1` por padrão de propósito — força o caminho de várias escritas, que é o
    que distingue este desenho de uma gravação única.
    """
    sink = await exporter.open_writer(query_id, columns, dataset_used)
    rows = list(rows)
    for start in range(0, len(rows), chunk_size):
        await sink.write(rows[start : start + chunk_size])
    await sink.close(
        StreamedResult(
            row_count=len(rows),
            total_rows=total_rows if total_rows is not None else len(rows),
            execution_ms=execution_ms,
        )
    )
    return sink


@pytest.fixture
def exporter(tmp_path) -> LocalFileResultExporter:
    return LocalFileResultExporter(tmp_path / "exports", ttl_seconds=3600)


async def _read(
    exporter: LocalFileResultExporter,
    query_id: str,
    kind: ExportKind = ExportKind.CSV,
) -> str:
    chunks = [chunk async for chunk in await exporter.open(query_id, kind)]
    return b"".join(chunks).decode("utf-8")


# --- escrita ------------------------------------------------------------------------------


async def test_grava_csv_jsonl_e_meta(exporter):
    await _export(exporter)

    assert await _read(exporter, "q_8f2a1c") == CSV_ESPERADO
    assert await _read(exporter, "q_8f2a1c", ExportKind.JSONL) == JSONL_ESPERADO

    meta = json.loads(await _read(exporter, "q_8f2a1c", ExportKind.META))
    assert meta["row_count"] == 2
    assert meta["total_rows"] == 2
    assert meta["dataset_used"] == "vendas_agregado_uf"
    assert [c["field"] for c in meta["columns"]] == ["sigla_uf", "valor_total"]


async def test_stat_devolve_metadados_do_artefato_pedido(exporter):
    await _export(exporter)

    csv_meta = await exporter.stat("q_8f2a1c", ExportKind.CSV)
    jsonl_meta = await exporter.stat("q_8f2a1c", ExportKind.JSONL)

    assert csv_meta.kind is ExportKind.CSV
    assert csv_meta.size_bytes == len(CSV_ESPERADO.encode("utf-8"))
    assert jsonl_meta.size_bytes == len(JSONL_ESPERADO.encode("utf-8"))

    # `created_at` é de cada arquivo, mas `expires_at` é do conjunto: os três vencem
    # juntos, ancorados no `.meta.json` (ver `test_ttl_do_conjunto_vem_do_meta_nao_do_csv`).
    meta_meta = await exporter.stat("q_8f2a1c", ExportKind.META)
    assert csv_meta.expires_at == jsonl_meta.expires_at == meta_meta.expires_at
    assert meta_meta.expires_at == meta_meta.created_at + timedelta(seconds=3600)


async def test_read_result_reconstroi_o_descritor_sem_as_linhas(exporter):
    """É o que deixa `GET /v1/query/{id}` responder depois de o job sumir do `arq`."""
    await _export(exporter, total_rows=4820113)

    result = await exporter.read_result("q_8f2a1c")

    assert result.rows is None
    assert result.columns == COLUNAS
    assert result.meta.row_count == 2
    assert result.meta.total_rows == 4820113
    assert result.meta.dataset_used == "vendas_agregado_uf"


async def test_read_result_de_export_inexistente_e_none(exporter):
    assert await exporter.read_result("q_000000") is None


async def test_cria_o_diretorio_se_nao_existir(tmp_path):
    destino = tmp_path / "ainda" / "nao" / "existe"
    exporter = LocalFileResultExporter(destino)

    await _export(exporter)

    assert (destino / "q_8f2a1c.csv").is_file()
    assert (destino / "q_8f2a1c.jsonl").is_file()
    assert (destino / "q_8f2a1c.meta.json").is_file()


async def test_substitui_o_anterior_do_mesmo_query_id(exporter):
    await _export(exporter)

    await _export(
        exporter,
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("MG",),),
    )

    assert await _read(exporter, "q_8f2a1c") == "sigla_uf\r\nMG\r\n"


async def test_nao_deixa_temporario_para_tras(exporter, tmp_path):
    await _export(exporter)

    assert sorted(p.name for p in (tmp_path / "exports").iterdir()) == [
        "q_8f2a1c.csv",
        "q_8f2a1c.jsonl",
        "q_8f2a1c.meta.json",
    ]


async def test_abort_nao_deixa_nada_visivel_nem_temporario(exporter, tmp_path):
    """Consulta que falha no meio: nenhum artefato pela metade fica servível."""
    sink = await exporter.open_writer("q_8f2a1c", COLUNAS, "vendas_agregado_uf")
    await sink.write([("SP", 1.0)])

    await sink.abort()

    assert list((tmp_path / "exports").iterdir()) == []
    assert await exporter.stat("q_8f2a1c") is None


async def test_meta_e_o_ultimo_a_aparecer(exporter, tmp_path):
    """A existência do `.meta.json` é a marca de export completo — enquanto o sink não
    fecha, nada do conjunto está visível."""
    sink = await exporter.open_writer("q_8f2a1c", COLUNAS, "vendas_agregado_uf")
    await sink.write(list(LINHAS))

    visiveis = [p.name for p in (tmp_path / "exports").iterdir() if not p.name.startswith(".")]
    assert visiveis == []

    await sink.close(StreamedResult(row_count=2, total_rows=2, execution_ms=1))
    assert (tmp_path / "exports" / "q_8f2a1c.meta.json").is_file()


async def test_ttl_precisa_ser_positivo(tmp_path):
    with pytest.raises(ValueError, match="positivo"):
        LocalFileResultExporter(tmp_path, ttl_seconds=0)


# --- travessia de caminho -----------------------------------------------------------------


@pytest.mark.parametrize(
    "query_id",
    ["../../etc/passwd", "q_../../x", "/etc/passwd", "q_ZZZZZZ", "", "q_8f2a1c/../x"],
)
async def test_query_id_invalido_nunca_vira_caminho(exporter, query_id):
    """`stat` de um `query_id` malformado é ausência, não erro — e abrir o sink recusa."""
    assert await exporter.stat(query_id) is None
    assert await exporter.read_result(query_id) is None

    with pytest.raises(InvalidQueryIdError):
        await exporter.open_writer(query_id, COLUNAS, "vendas_agregado_uf")


# --- leitura e TTL ------------------------------------------------------------------------


async def test_stat_de_export_inexistente_e_none(exporter):
    assert await exporter.stat("q_000000") is None


async def test_stat_de_export_vencido_e_none(tmp_path):
    """O TTL é autoritativo na leitura: vencido some da resposta antes de a varredura
    passar por ele."""
    exporter = LocalFileResultExporter(tmp_path, ttl_seconds=3600)
    await _export(exporter)
    _envelhecer(tmp_path / "q_8f2a1c.meta.json", horas=2)

    assert await exporter.stat("q_8f2a1c") is None
    assert (tmp_path / "q_8f2a1c.csv").is_file()  # ainda no disco, só não é servido


async def test_ttl_do_conjunto_vem_do_meta_nao_do_csv(tmp_path):
    """O CSV de uma consulta longa é escrito muito antes do fim. Se cada arquivo vencesse
    pelo próprio `mtime`, o download expiraria antes do descritor que o descreve."""
    exporter = LocalFileResultExporter(tmp_path, ttl_seconds=3600)
    await _export(exporter)
    _envelhecer(tmp_path / "q_8f2a1c.csv", horas=2)  # só o CSV é "velho"

    assert await exporter.stat("q_8f2a1c", ExportKind.CSV) is not None


async def test_open_de_export_inexistente_levanta(exporter):
    with pytest.raises(FileNotFoundError):
        await exporter.open("q_000000")


async def test_open_sobrevive_a_remocao_durante_a_leitura(exporter, tmp_path):
    """O descritor é aberto antes do gerador: uma varredura concorrente não trunca um
    download em andamento."""
    await _export(exporter)
    stream = await exporter.open("q_8f2a1c")

    (tmp_path / "exports" / "q_8f2a1c.csv").unlink()

    conteudo = b"".join([chunk async for chunk in stream]).decode("utf-8")
    assert conteudo == CSV_ESPERADO


async def test_leitura_em_blocos_de_arquivo_grande(tmp_path):
    """Mais de um bloco de 64 KiB — o caminho que existe para não materializar."""
    exporter = LocalFileResultExporter(tmp_path)
    await _export(
        exporter,
        "q_abcdef",
        columns=(Column(field="valor", type=DataType.STRING),),
        rows=[(f"linha-{i:06d}",) for i in range(20_000)],
        chunk_size=1000,
        dataset_used="vendas_detalhado",
    )

    metadata = await exporter.stat("q_abcdef")
    conteudo = await _read(exporter, "q_abcdef")

    assert metadata.size_bytes > 64 * 1024
    assert conteudo.startswith("valor\r\nlinha-000000\r\n")
    assert conteudo.endswith("linha-019999\r\n")


# --- varredura ----------------------------------------------------------------------------


async def test_purge_remove_so_os_vencidos(tmp_path):
    exporter = LocalFileResultExporter(tmp_path, ttl_seconds=3600)
    await _export(exporter, "q_aaaaaa")
    await _export(exporter, "q_bbbbbb")
    _envelhecer(tmp_path / "q_aaaaaa.meta.json", horas=2)

    removidos = await exporter.purge_expired()

    assert removidos == 1
    assert (tmp_path / "q_bbbbbb.csv").is_file()


async def test_purge_remove_os_tres_artefatos_juntos(tmp_path):
    exporter = LocalFileResultExporter(tmp_path, ttl_seconds=3600)
    await _export(exporter)
    _envelhecer(tmp_path / "q_8f2a1c.meta.json", horas=2)

    await exporter.purge_expired()

    assert not (tmp_path / "q_8f2a1c.csv").exists()
    assert not (tmp_path / "q_8f2a1c.jsonl").exists()
    assert not (tmp_path / "q_8f2a1c.meta.json").exists()


async def test_purge_e_idempotente(tmp_path):
    exporter = LocalFileResultExporter(tmp_path, ttl_seconds=3600)
    await _export(exporter)
    _envelhecer(tmp_path / "q_8f2a1c.meta.json", horas=2)

    assert await exporter.purge_expired() == 1
    assert await exporter.purge_expired() == 0


async def test_purge_em_diretorio_inexistente_nao_falha(tmp_path):
    exporter = LocalFileResultExporter(tmp_path / "nunca-criado")

    assert await exporter.purge_expired() == 0


def _envelhecer(path, *, horas: int) -> None:
    """Recua o `mtime` do arquivo — é o que o exportador usa como data de criação."""
    quando = (datetime.now(UTC) - timedelta(hours=horas)).timestamp()
    os.utime(path, (quando, quando))
