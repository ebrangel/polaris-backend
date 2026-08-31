"""`LocalFileResultExporter` — implementa `ResultExporter` (seção 2.4a) sobre o
filesystem.

Um arquivo `<export_dir>/<query_id>.csv` por consulta pesada concluída, escrito pelo
worker e servido pela API.

**Limitação a conhecer antes de usar em produção:** worker e API precisam enxergar o
mesmo `export_dir` — mesmo host, ou um volume compartilhado. Num deploy multi-nó sem
volume comum, a API não acha o arquivo que outro nó escreveu. Esse é exatamente o buraco
que um adapter de S3 fecha, e fechá-lo não muda contrato nenhum: o port continua o
mesmo, e a URL de download continua sendo a da própria API.
"""

import asyncio
import logging
import os
import re
import tempfile
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from adapters.csv_format import csv_lines
from application.ports.result_exporter import ExportMetadata
from domain.models import QueryResult, QueryStatus

logger = logging.getLogger(__name__)

#: Formato de `QueryRequest.query_id` (`q_8f2a1c`, seção 2.3). O `query_id` vira nome de
#: arquivo, então é validado antes de tocar o disco — sem isso, um identificador com
#: `../` escaparia do `export_dir`. O limite superior é folgado de propósito: o domínio
#: hoje corta o hash em 6 caracteres, e aumentar esse corte não deveria quebrar o export.
_QUERY_ID = re.compile(r"^q_[0-9a-f]{6,64}$")

_SUFFIX = ".csv"

#: Tamanho de bloco da leitura. Grande o bastante para que o custo por bloco não domine
#: num arquivo de dezenas de MB, pequeno o bastante para não anular o ganho de servir o
#: download sem materializar.
_CHUNK_BYTES = 64 * 1024


class InvalidQueryIdError(ValueError):
    """`query_id` fora do formato da seção 2.3 — nunca vira caminho de arquivo."""


class LocalFileResultExporter:
    """Exporta `QueryResult` como CSV em disco, com TTL por arquivo.

    Todo o I/O de arquivo é síncrono, então cada operação vai para `asyncio.to_thread`:
    o worker roda a limpeza e a escrita no mesmo event loop que consome a fila, e a API
    serve o download no loop que atende as requisições — nenhum dos dois pode bloquear.
    """

    def __init__(self, export_dir: str | Path, ttl_seconds: int = 86_400) -> None:
        if ttl_seconds <= 0:
            raise ValueError(f"`ttl_seconds` precisa ser positivo: {ttl_seconds}.")
        self._export_dir = Path(export_dir)
        self._ttl = timedelta(seconds=ttl_seconds)

    def _path_for(self, query_id: str) -> Path:
        if not _QUERY_ID.match(query_id):
            raise InvalidQueryIdError(
                f"'{query_id}' não é um `query_id` válido — o export não é acessível."
            )
        return self._export_dir / f"{query_id}{_SUFFIX}"

    def _metadata_for(self, query_id: str, stat: os.stat_result) -> ExportMetadata:
        created_at = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
        return ExportMetadata(
            query_id=query_id,
            size_bytes=stat.st_size,
            created_at=created_at,
            expires_at=created_at + self._ttl,
        )

    # --- escrita ---------------------------------------------------------------------

    def _write_sync(self, result: QueryResult) -> ExportMetadata:
        """Grava num temporário e move por cima com `os.replace`.

        A troca é atômica dentro do mesmo filesystem, então um download concorrente
        nunca enxerga um arquivo pela metade: ou o export anterior, ou o novo inteiro.
        Daí o temporário nascer no próprio `export_dir`, e não em `/tmp` (que pode estar
        noutro filesystem, onde `os.replace` deixaria de ser atômico).
        """
        path = self._path_for(result.query_id)
        self._export_dir.mkdir(parents=True, exist_ok=True)

        descriptor, temporary_name = tempfile.mkstemp(
            dir=self._export_dir, prefix=f".{result.query_id}.", suffix=_SUFFIX
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
                for line in csv_lines(result):
                    handle.write(line)
            os.replace(temporary, path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

        return self._metadata_for(result.query_id, path.stat())

    async def export(self, result: QueryResult) -> ExportMetadata:
        if result.status is not QueryStatus.COMPLETED:
            raise ValueError("só resultados com status=completed são exportáveis")
        return await asyncio.to_thread(self._write_sync, result)

    # --- leitura ---------------------------------------------------------------------

    def _stat_sync(self, query_id: str) -> ExportMetadata | None:
        try:
            stat = self._path_for(query_id).stat()
        except (FileNotFoundError, NotADirectoryError, InvalidQueryIdError):
            return None

        metadata = self._metadata_for(query_id, stat)
        if metadata.expires_at <= datetime.now(UTC):
            # Vencido deixa de ser servido na hora, mesmo que a varredura ainda não
            # tenha passado por ele — quem manda é o TTL, não o cron.
            return None
        return metadata

    async def stat(self, query_id: str) -> ExportMetadata | None:
        return await asyncio.to_thread(self._stat_sync, query_id)

    async def open(self, query_id: str) -> AsyncIterator[bytes]:
        """Abre o arquivo **agora** e devolve um gerador que lê em blocos.

        O descritor é aberto antes de o gerador existir, de propósito: no POSIX um
        `unlink` não invalida um descritor já aberto, então uma varredura que apague o
        arquivo no meio do download não trunca a resposta.
        """
        handle = await asyncio.to_thread(self._path_for(query_id).open, "rb")

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
        for entry in self._export_dir.iterdir():
            if entry.suffix != _SUFFIX or not entry.is_file():
                continue
            try:
                if datetime.fromtimestamp(entry.stat().st_mtime, tz=UTC) > deadline:
                    continue
                entry.unlink()
            except FileNotFoundError:
                # Outra varredura (ou outro processo) chegou primeiro — não é erro.
                continue
            removed += 1
        return removed

    async def purge_expired(self) -> int:
        removed = await asyncio.to_thread(self._purge_sync)
        if removed:
            logger.info("exports expirados removidos: %d", removed)
        return removed
