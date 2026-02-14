"""
Repository layer for data access.

Uses async MongoDB via motor for all persistence.

Architecture:
    MongoDBConnection
        ├── .datasets collection → DatasetRepository
        ├── .users collection
        └── .search_history collection

Usage:
    from etl.repository import MongoDBConnection, DatasetRepository

    conn = MongoDBConnection()
    await conn.connect()

    repo = DatasetRepository(conn.datasets)
    dataset = await repo.get("abc-123")
    results = await repo.search("climate")
"""

from .base import (
    BulkOperationResult,
    PagedResult,
)
from .mongodb import (
    MongoDBConfig,
    MongoDBConnection,
    get_connection,
    get_database,
    reset_connection,
)
from .dataset_repository import DatasetRepository


__all__ = [
    # Base classes
    "BulkOperationResult",
    "PagedResult",

    # MongoDB connection
    "MongoDBConfig",
    "MongoDBConnection",
    "get_connection",
    "get_database",
    "reset_connection",

    # Concrete repositories
    "DatasetRepository",
]
