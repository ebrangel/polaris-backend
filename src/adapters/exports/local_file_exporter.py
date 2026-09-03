"""`LocalFileResultExporter` — implementa `ResultExporter` (seções 2.3a e 2.4a) sobre o
filesystem.

Três arquivos por consulta concluída, escritos pelo worker **enquanto o cursor é lido** e
servidos pela API:

```
<export_dir>/<query_id>.csv         download e ?format=csv
<export_dir>/<query_id>.jsonl       corpo JSON transmitido, uma lista por linha
<export_dir>/<query_id>.meta.json   colunas + meta, gravado por último
```

O `.meta.json` sair por último não é detalhe: sua existência é o sinal de que o export
está completo, e é o que deixa `GET /v1/query/{query_id}` responder mesmo depois de o
resultado retido do `arq` ter expirado.

**Limitação a conhecer antes de usar em produção:** worker e API precisam enxergar o
mesmo `export_dir` — mesmo host, ou um volume compartilhado. Num deploy multi-nó sem
volume comum, a API não acha o arquivo que outro nó escreveu. Esse é exatamente o buraco
que um adapter de S3 fecha, e fechá-lo não muda contrato nenhum: o port continua o
mesmo, e a URL de download continua sendo a da própria API.
"""

import asyncio
import json
import logging
import os
import re
import tempfile
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import IO, Any

from adapters.csv_format import CsvRowFormatter
from adapters.serialization import jsonable
from application.ports.result_exporter import ExportKind, ExportMetadata
from application.ports.row_sink import StreamedResult
from domain.models import Column, DataType, QueryResult

logger = logging.getLogger(__name__)

#: Formato de `QueryRequest.query_id` (`q_8f2a1c`, seção 2.3). O `query_id` vira nome de
#: arquivo, então é validado antes de tocar o disco — sem isso, um identificador com
#: `../` escaparia do `export_dir`. O limite superior é folgado de propósito: o domínio
#: hoje corta o hash em 6 caracteres, e aumentar esse corte não deveria quebrar o export.
_QUERY_ID = re.compile(r"^q_[0-9a-f]{6,64}$")

_SUFFIXES = {
    ExportKind.CSV: ".csv",
    ExportKind.JSONL: ".jsonl",
    ExportKind.META: ".meta.json",
}

#: Tamanho de bloco da leitura. Grande o bastante para que o custo por bloco não domine
#: num arquivo de dezenas de MB, pequeno o bastante para não anular o ganho de servir o
#: download sem materializar.
_CHUNK_BYTES = 64 * 1024


class InvalidQueryIdError(ValueError):
    """`query_id` fora do formato da seção 2.3 — nunca vira caminho de arquivo."""


def _path_for(export_dir: Path, query_id: str, kind: ExportKind) -> Path:
    if not _QUERY_ID.match(query_id):
        raise InvalidQueryIdError(
            f"'{query_id}' não é um `query_id` válido — o export não é acessível."
        )
    return export_dir / f"{query_id}{_SUFFIXES[kind]}"


class _LocalFileSink:
    """Escreve os três artefatos de uma consulta, bloco a bloco.

    Cada arquivo nasce como temporário dentro do próprio `export_dir` e só é movido para
    o nome definitivo no `close`, com `os.replace`. A troca é atômica dentro do mesmo
    filesystem, então um download concorrente nunca enxerga um arquivo pela metade: ou o
    export anterior, ou o novo inteiro. Daí o temporário não nascer em `/tmp`, que pode
    estar noutro filesystem, onde `os.replace` deixaria de ser atômico.

    Todo o I/O de arquivo é síncrono, então vai para `asyncio.to_thread` — mas **uma vez
    por bloco de linhas**, não por linha: o worker consome o cursor no mesmo event loop
    em que escreve, e um salto de thread por linha dominaria o custo.
    """

    __slots__ = (
        "_export_dir", "_query_id", "_columns", "_dataset_used", "_open", "_csv", "_closed"
    )

    def __init__(
        self,
        export_dir: Path,
        query_id: str,
        columns: tuple[Column, ...],
        dataset_used: str,
    ) -> None:
        # Valida o `query_id` já na construção: melhor falhar antes de abrir arquivo
        # nenhum do que deixar um temporário órfão para trás.
        _path_for(export_dir, query_id, ExportKind.CSV)
        self._export_dir = export_dir
        self._query_id = query_id
        self._columns = columns
        self._dataset_used = dataset_used
        self._open: dict[ExportKind, tuple[IO[str], Path]] = {}
        self._csv = CsvRowFormatter()
        self._closed = False

    # --- abertura ---------------------------------------------------------------------

    def _open_sync(self) -> None:
        self._export_dir.mkdir(parents=True, exist_ok=True)
        for kind in (ExportKind.CSV, ExportKind.JSONL):
            descriptor, name = tempfile.mkstemp(
                dir=self._export_dir,
                prefix=f".{self._query_id}.",
                suffix=_SUFFIXES[kind],
            )
            self._open[kind] = (
                os.fdopen(descriptor, "w", encoding="utf-8", newline=""),
                Path(name),
            )
        self._open[ExportKind.CSV][0].write(self._csv.header(self._columns))

    async def open(self) -> None:
        await asyncio.to_thread(self._open_sync)

    # --- escrita ----------------------------------------------------------------------

    def _write_sync(self, rows: Sequence[tuple[Any, ...]]) -> None:
        csv_handle = self._open[ExportKind.CSV][0]
        jsonl_handle = self._open[ExportKind.JSONL][0]
        # Uma string por bloco em vez de uma escrita por linha: o buffer é do tamanho do
        # bloco (mil linhas por padrão), não do resultado.
        csv_handle.write("".join(self._csv.row(row) for row in rows))
        jsonl_handle.write(
            "".join(
                json.dumps([jsonable(value) for value in row], ensure_ascii=False) + "\n"
                for row in rows
            )
        )

    async def write(self, rows: Sequence[tuple[Any, ...]]) -> None:
        if self._closed or not rows:
            return
        await asyncio.to_thread(self._write_sync, rows)

    # --- finalização ------------------------------------------------------------------

    def _close_sync(self, result: StreamedResult) -> None:
        for kind in (ExportKind.CSV, ExportKind.JSONL):
            handle, temporary = self._open[kind]
            handle.close()
            os.replace(temporary, _path_for(self._export_dir, self._query_id, kind))

        # O `.meta.json` é o último a aparecer, e por isso serve de marca de export
        # completo: quem encontra o meta sabe que os outros dois já estão inteiros.
        meta_path = _path_for(self._export_dir, self._query_id, ExportKind.META)
        descriptor, name = tempfile.mkstemp(
            dir=self._export_dir,
            prefix=f".{self._query_id}.",
            suffix=_SUFFIXES[ExportKind.META],
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "query_id": self._query_id,
                    "columns": [
                        {"field": c.field, "type": c.type.value, "format": c.format}
                        for c in self._columns
                    ],
                    "row_count": result.row_count,
                    "total_rows": result.total_rows,
                    "execution_ms": result.execution_ms,
                    "dataset_used": self._dataset_used,
                },
                handle,
                ensure_ascii=False,
            )
        os.replace(name, meta_path)

    async def close(self, result: StreamedResult) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.to_thread(self._close_sync, result)

    def _abort_sync(self) -> None:
        for handle, temporary in self._open.values():
            try:
                handle.close()
            except OSError:
                pass
            temporary.unlink(missing_ok=True)

    async def abort(self) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.to_thread(self._abort_sync)


class LocalFileResultExporter:
    """Exporta resultados como arquivos em disco, com TTL por consulta."""

    def __init__(self, export_dir: str | Path, ttl_seconds: int = 86_400) -> None:
        if ttl_seconds <= 0:
            raise ValueError(f"`ttl_seconds` precisa ser positivo: {ttl_seconds}.")
        self._export_dir = Path(export_dir)
        self._ttl = timedelta(seconds=ttl_seconds)

    # --- escrita ---------------------------------------------------------------------

    async def open_writer(
        self, query_id: str, columns: tuple[Column, ...], dataset_used: str
    ) -> _LocalFileSink:
        sink = _LocalFileSink(self._export_dir, query_id, columns, dataset_used)
        await sink.open()
        return sink

    # --- leitura ---------------------------------------------------------------------

    def _stat_sync(self, query_id: str, kind: ExportKind) -> ExportMetadata | None:
        try:
            stat = _path_for(self._export_dir, query_id, kind).stat()
            # O vencimento do conjunto é o do `.meta.json`, que é o último a ser escrito:
            # os três arquivos de uma consulta vencem juntos, e ancorar cada um no próprio
            # mtime faria o CSV (escrito primeiro, e possivelmente por muitos minutos)
            # expirar antes do descritor que o descreve.
            meta_stat = _path_for(self._export_dir, query_id, ExportKind.META).stat()
        except (FileNotFoundError, NotADirectoryError, InvalidQueryIdError):
            return None

        expires_at = datetime.fromtimestamp(meta_stat.st_mtime, tz=UTC) + self._ttl
        if expires_at <= datetime.now(UTC):
            # Vencido deixa de ser servido na hora, mesmo que a varredura ainda não
            # tenha passado por ele — quem manda é o TTL, não o cron.
            return None
        return ExportMetadata(
            query_id=query_id,
            kind=kind,
            size_bytes=stat.st_size,
            created_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
            expires_at=expires_at,
        )

    async def stat(
        self, query_id: str, kind: ExportKind = ExportKind.CSV
    ) -> ExportMetadata | None:
        return await asyncio.to_thread(self._stat_sync, query_id, kind)

    def _read_result_sync(self, query_id: str) -> QueryResult | None:
        if self._stat_sync(query_id, ExportKind.META) is None:
            return None
        try:
            raw = _path_for(self._export_dir, query_id, ExportKind.META).read_text("utf-8")
        except (FileNotFoundError, NotADirectoryError, InvalidQueryIdError):
            return None

        data = json.loads(raw)
        return QueryResult.streamed(
            query_id=data["query_id"],
            columns=tuple(
                Column(field=c["field"], type=DataType(c["type"]), format=c.get("format"))
                for c in data["columns"]
            ),
            row_count=data["row_count"],
            total_rows=data.get("total_rows"),
            dataset_used=data["dataset_used"],
            execution_ms=data["execution_ms"],
        )

    async def read_result(self, query_id: str) -> QueryResult | None:
        return await asyncio.to_thread(self._read_result_sync, query_id)

    async def open(
        self, query_id: str, kind: ExportKind = ExportKind.CSV
    ) -> AsyncIterator[bytes]:
        """Abre o arquivo **agora** e devolve um gerador que lê em blocos.

        O descritor é aberto antes de o gerador existir, de propósito: no POSIX um
        `unlink` não invalida um descritor já aberto, então uma varredura que apague o
        arquivo no meio do download não trunca a resposta.
        """
        path = _path_for(self._export_dir, query_id, kind)
        handle = await asyncio.to_thread(path.open, "rb")

        async def chunks() -> AsyncIterator[bytes]:
            try:
                while True:
                    block = await asyncio.to_thread(handle.read, _CHUNK_BYTES)
                    if not block:
                        return
                    yield block
            finally:
                await asyncio.to_thread(handle.close)

        return chunks()

    # --- limpeza ---------------------------------------------------------------------

    def _purge_sync(self) -> int:
        if not self._export_dir.is_dir():
            return 0

        deadline = datetime.now(UTC) - self._ttl
        removed = 0
        for entry in self._export_dir.glob("*.meta.json"):
            query_id = entry.name[: -len(_SUFFIXES[ExportKind.META])]
            try:
                if datetime.fromtimestamp(entry.stat().st_mtime, tz=UTC) > deadline:
                    continue
                # O conjunto é apagado com o meta por último, na ordem inversa da
                # escrita: enquanto o meta existir, o export conta como completo.
                for kind in (ExportKind.CSV, ExportKind.JSONL, ExportKind.META):
                    _path_for(self._export_dir, query_id, kind).unlink(missing_ok=True)
            except (FileNotFoundError, InvalidQueryIdError):
                # Outra varredura chegou primeiro, ou um arquivo com nome fora do padrão
                # — nenhum dos dois é erro.
                continue
            removed += 1
        return removed

    async def purge_expired(self) -> int:
        removed = await asyncio.to_thread(self._purge_sync)
        if removed:
            logger.info("exports expirados removidos: %d", removed)
        return removed
