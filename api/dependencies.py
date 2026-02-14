"""
FastAPI Dependencies.

Dependency injection for MongoDB + SentenceTransformers stack.
Uses lifespan-scoped singletons for expensive resources.
"""

import os
from typing import Annotated, Optional

from dotenv import load_dotenv
load_dotenv()
from fastapi import Depends

from etl.embeddings.vector_store import VectorStore
from etl.repository.dataset_repository import DatasetRepository
from etl.repository.mongodb import MongoDBConnection, MongoDBConfig
from etl.search.hybrid import HybridSearchService

# Will be set during app lifespan
_mongo_conn: Optional[MongoDBConnection] = None
_vector_store: Optional[VectorStore] = None
_embedding_service = None
_hybrid_search: Optional[HybridSearchService] = None
_dataset_repo: Optional[DatasetRepository] = None


# =============================================================================
# Initialization (called from lifespan)
# =============================================================================

async def init_dependencies():
    """
    Initialize all dependencies.

    Called once during app startup via lifespan.
    """
    global _mongo_conn, _vector_store, _embedding_service, _hybrid_search, _dataset_repo

    # MongoDB connection
    config = MongoDBConfig(
        uri=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        database_name=os.getenv("MONGODB_DATABASE", "dsh_etl_search"),
    )
    _mongo_conn = MongoDBConnection(config)
    await _mongo_conn.connect()

    # Create indexes
    await _mongo_conn.create_indexes()

    # Dataset repository
    _dataset_repo = DatasetRepository(_mongo_conn.datasets)

    # Embedding service (local, free)
    try:
        from etl.embeddings import SentenceTransformerService
        model_name = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
        _embedding_service = SentenceTransformerService(model_name=model_name)
    except Exception as e:
        print(f"Warning: Failed to init embedding service: {e}")
        _embedding_service = None

    # Vector store (uses same MongoDB collection as datasets)
    if _embedding_service:
        try:
            _vector_store = VectorStore(
                embedding_service=_embedding_service,
                collection=_mongo_conn.datasets,
            )
        except Exception as e:
            print(f"Warning: Failed to init vector store: {e}")
            _vector_store = None

    # Hybrid search
    if _vector_store and _dataset_repo:
        _hybrid_search = HybridSearchService(
            vector_store=_vector_store,
            repository=_dataset_repo,
        )


async def shutdown_dependencies():
    """
    Cleanup dependencies.

    Called during app shutdown via lifespan.
    """
    global _mongo_conn, _vector_store, _embedding_service, _hybrid_search, _dataset_repo

    _hybrid_search = None
    _vector_store = None
    _embedding_service = None
    _dataset_repo = None

    if _mongo_conn:
        await _mongo_conn.close()
        _mongo_conn = None


# =============================================================================
# Dependency Getters
# =============================================================================

def get_dataset_repository() -> DatasetRepository:
    """Get dataset repository instance."""
    if _dataset_repo is None:
        raise RuntimeError("Database not initialized")
    return _dataset_repo


def get_vector_store() -> Optional[VectorStore]:
    """Get vector store (None if not configured)."""
    return _vector_store


def get_embedding_service():
    """Get embedding service (None if not configured)."""
    return _embedding_service


def get_hybrid_search() -> Optional[HybridSearchService]:
    """Get hybrid search service (None if not configured)."""
    return _hybrid_search


def get_mongo_connection() -> MongoDBConnection:
    """Get MongoDB connection instance."""
    if _mongo_conn is None:
        raise RuntimeError("Database not initialized")
    return _mongo_conn


# =============================================================================
# Status Helpers
# =============================================================================

async def get_service_status() -> dict:
    """Get status of all services."""
    status = {
        "database": _mongo_conn is not None,
        "vector_store": _vector_store is not None,
        "embedding_service": _embedding_service is not None,
        "dataset_count": 0,
        "indexed_count": 0,
    }

    if _dataset_repo:
        try:
            status["dataset_count"] = await _dataset_repo.count()
        except Exception:
            pass

    if _vector_store:
        try:
            indexed_ids = await _vector_store.get_indexed_ids()
            status["indexed_count"] = len(indexed_ids)
        except Exception:
            pass

    return status


# =============================================================================
# Type Aliases for Depends()
# =============================================================================

DatasetRepoDep = Annotated[DatasetRepository, Depends(get_dataset_repository)]
VectorStoreDep = Annotated[Optional[VectorStore], Depends(get_vector_store)]
HybridSearchDep = Annotated[Optional[HybridSearchService], Depends(get_hybrid_search)]
