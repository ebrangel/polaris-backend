"""Use cases — orquestram entidades de domínio e, quando fazem I/O, os ports."""

from application.use_cases.execute_query import ExecuteQuery
from application.use_cases.resolve_dataset import ResolveDataset

__all__ = ["ExecuteQuery", "ResolveDataset"]
