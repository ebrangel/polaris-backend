"""Entry point dos dois processos do sistema — toda a montagem de verdade está em
`infrastructure/bootstrap.py`; este módulo só decide qual metade construir.

- `uvicorn main:app` — sobe a API (`PROCESS_ROLE=api`, o padrão).
- `arq main.WorkerSettings` — sobe o worker que consome a fila (Marco 7), com
  `PROCESS_ROLE=worker` no ambiente do processo.

Os dois entry points importam este módulo inteiro; sem o `PROCESS_ROLE`, o import de
qualquer um dos dois bootaria a montagem inteira duas vezes (conexões dobradas).

As duas fábricas são **síncronas**, e isso é obrigatório: o uvicorn importa `main:app`
de dentro do event loop dele (`Server.serve` → `config.load()`), então um `asyncio.run()`
aqui estouraria com "asyncio.run() cannot be called from a running event loop". Todo o
I/O do boot (catálogo do Postgres, pools do Redis, engines por datasource) acontece no
`lifespan` da app e no `on_startup` do worker, já dentro do loop de cada processo.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from infrastructure.bootstrap import create_application, create_worker_settings  # noqa: E402

_role = os.environ.get("PROCESS_ROLE", "api")

if _role == "api":
    app = create_application()
elif _role == "worker":
    WorkerSettings = create_worker_settings()
else:
    raise RuntimeError(f"PROCESS_ROLE desconhecido: {_role!r} — use 'api' ou 'worker'.")
