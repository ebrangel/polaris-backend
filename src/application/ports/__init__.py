"""Contratos que `adapters/` implementa — ainda sem implementação concreta (Marco 2)."""

from application.ports.cache_gateway import CacheGateway
from application.ports.catalog_repository import CatalogRepository
from application.ports.job_queue import JobQueue
from application.ports.query_executor import ExecutionProfile, QueryCost, QueryExecutor

__all__ = [
    "CacheGateway",
    "CatalogRepository",
    "ExecutionProfile",
    "JobQueue",
    "QueryCost",
    "QueryExecutor",
]
