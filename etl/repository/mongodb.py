"""
MongoDB connection manager.

Replaces SQLAlchemy SessionFactory/UnitOfWork with async MongoDB via motor.

Usage:
    from etl.repository.mongodb import MongoDBConnection

    conn = MongoDBConnection()
    await conn.connect()

    # Access collections
    datasets = conn.datasets
    users = conn.users

    # Cleanup
    await conn.close()
"""

from __future__ import annotations

import os
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection


class MongoDBConfig:
    """MongoDB configuration from environment."""

    def __init__(
        self,
        uri: Optional[str] = None,
        database_name: Optional[str] = None,
    ):
        self.uri = uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.database_name = database_name or os.getenv("MONGODB_DATABASE", "dsh_etl_search")


class MongoDBConnection:
    """
    Async MongoDB connection manager using motor.

    Wraps AsyncIOMotorClient and provides convenient access
    to the database and collections.
    """

    def __init__(self, config: Optional[MongoDBConfig] = None):
        self.config = config or MongoDBConfig()
        self._client: Optional[AsyncIOMotorClient] = None
        self._db: Optional[AsyncIOMotorDatabase] = None

    async def connect(self) -> None:
        """Connect to MongoDB."""
        self._client = AsyncIOMotorClient(self.config.uri)
        self._db = self._client[self.config.database_name]
        # Verify connection
        await self._client.admin.command("ping")

    async def close(self) -> None:
        """Close the MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None

    @property
    def db(self) -> AsyncIOMotorDatabase:
        """The database instance."""
        if self._db is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._db

    @property
    def datasets(self) -> AsyncIOMotorCollection:
        """The datasets collection."""
        return self.db["datasets"]

    @property
    def users(self) -> AsyncIOMotorCollection:
        """The users collection."""
        return self.db["users"]

    @property
    def pending(self) -> AsyncIOMotorCollection:
        """The pending uploads collection."""
        return self.db["pending"]

    @property
    def search_history(self) -> AsyncIOMotorCollection:
        """The search_history collection."""
        return self.db["search_history"]

    async def create_indexes(self) -> None:
        """Create standard indexes for all collections."""
        # Text index for keyword search on datasets
        await self.datasets.create_index(
            [("title", "text"), ("abstract", "text")],
            name="text_search",
        )
        # Unique index on identifier
        await self.datasets.create_index("identifier", unique=True)

        # Users
        await self.users.create_index("email", unique=True)

        # Pending uploads
        await self.pending.create_index("uploaded_at")
        await self.pending.create_index("uploaded_by")

        # Search history
        await self.search_history.create_index("searched_at")
        await self.search_history.create_index("user_id")


# Singleton
_connection: Optional[MongoDBConnection] = None


def get_connection(config: Optional[MongoDBConfig] = None) -> MongoDBConnection:
    """Get or create the default MongoDB connection."""
    global _connection
    if _connection is None:
        _connection = MongoDBConnection(config)
    return _connection


async def get_database() -> AsyncIOMotorDatabase:
    """Convenience: get the database from the default connection."""
    conn = get_connection()
    if conn._db is None:
        await conn.connect()
    return conn.db


def reset_connection() -> None:
    """Reset the default connection (for testing)."""
    global _connection
    _connection = None
