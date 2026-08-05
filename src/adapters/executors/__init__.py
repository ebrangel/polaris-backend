"""Executores de consulta — implementações do port `QueryExecutor` (Marco 2)."""

from adapters.executors.elasticsearch_executor import ElasticsearchQueryExecutor
from adapters.executors.sqlalchemy_executor import SQLAlchemyQueryExecutor

__all__ = ["ElasticsearchQueryExecutor", "SQLAlchemyQueryExecutor"]
