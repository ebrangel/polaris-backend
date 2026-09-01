"""Contratos que `adapters/` implementa."""

from application.ports.cache_gateway import CacheGateway, CacheStats
from application.ports.catalog_invalidator import CatalogInvalidator
from application.ports.catalog_repository import CatalogRepository
from application.ports.datasource_inspector import DatasourceInspector
from application.ports.job_queue import JobQueue
from application.ports.query_executor import QueryExecutor
from application.ports.rate_limiter import RateLimiter
from application.ports.result_exporter import ExportMetadata, ResultExporter

__all__ = [
    "CacheGateway",
    "CacheStats",
    "CatalogInvalidator",
    "CatalogRepository",
    "DatasourceInspector",
    "ExportMetadata",
    "JobQueue",
    "QueryExecutor",
    "RateLimiter",
    "ResultExporter",
]
