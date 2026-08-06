"""Contratos que `adapters/` implementa."""

from application.ports.cache_gateway import CacheGateway
from application.ports.catalog_invalidator import CatalogInvalidator
from application.ports.catalog_repository import CatalogRepository
from application.ports.datasource_inspector import DatasourceInspector
from application.ports.job_queue import JobQueue
from application.ports.query_executor import ExecutionProfile, QueryCost, QueryExecutor

__all__ = [
    "CacheGateway",
    "CatalogInvalidator",
    "CatalogRepository",
    "DatasourceInspector",
    "ExecutionProfile",
    "JobQueue",
    "QueryCost",
    "QueryExecutor",
]
