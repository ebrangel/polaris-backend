"""Resposta HTTP em CSV — o segundo presenter da seção 2.3a.

Fica ao lado de `present_result()` (`schemas.py`, o presenter JSON) e é a única peça
que o CSV acrescenta ao caminho da consulta: `QueryResult` já é uma tabela
(`columns` + `rows`), então nada muda em `domain/`, `application/` nem nos executores.

A escrita do CSV em si mora em `adapters/csv_format.py`, compartilhada com o worker
(que grava o mesmo formato em arquivo, seção 2.4a). Aqui fica só o que é HTTP: nome do
arquivo baixado e os headers que carregam o `meta` fora de banda.

Módulo puro (sem FastAPI), como `query_params.py` e `content_negotiation.py`.
"""

from adapters.csv_format import CsvRowFormatter, csv_lines
from domain.models import QueryResult

__all__ = ["CsvRowFormatter", "csv_filename", "csv_headers", "csv_lines"]


def csv_filename(query_id: str) -> str:
    """Nome do arquivo baixado. O `query_id` identifica a consulta e é estável entre
    requisições idênticas, o que dá nomes reproduzíveis."""
    return f"{query_id}.csv"


def csv_headers(result: QueryResult) -> dict[str, str]:
    """Headers da resposta CSV, incluindo o `meta` da seção 2.3.

    O CSV é uma grade de dados e não tem onde carregar `dataset_used`, `cached` ou
    `execution_ms` sem estragar o arquivo para quem o abre numa planilha — então esses
    metadados saem fora de banda, em headers. A observabilidade do Marco 9 continua
    disponível para quem consome a API por código.
    """
    headers = {
        "Content-Disposition": f'attachment; filename="{csv_filename(result.query_id)}"',
        "X-Query-Id": result.query_id,
    }
    if result.meta is not None:
        headers["X-Row-Count"] = str(result.meta.row_count)
        headers["X-Cached"] = "true" if result.meta.cached else "false"
        headers["X-Execution-Ms"] = str(result.meta.execution_ms)
        headers["X-Dataset-Used"] = result.meta.dataset_used
        if result.meta.total_rows is not None:
            # Ao contrário do `meta` em JSON, aqui o total ausente é a **omissão** do
            # header: não existe `null` em HTTP, e um `X-Total-Rows: ` vazio seria lido
            # como zero por quem consome. Sem o header, o cliente sabe que não sabe.
            headers["X-Total-Rows"] = str(result.meta.total_rows)
    return headers
