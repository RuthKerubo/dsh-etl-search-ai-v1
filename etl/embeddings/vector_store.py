"""
Vector Store with MongoDB Atlas.

Stores embeddings as a field on dataset documents and uses
MongoDB Atlas $vectorSearch for semantic similarity search.

Usage:
    from etl.embeddings import SentenceTransformerService, VectorStore
    from etl.repository import MongoDBConnection

    conn = MongoDBConnection()
    await conn.connect()

    embedding_service = SentenceTransformerService()
    store = VectorStore(
        embedding_service=embedding_service,
        collection=conn.datasets,
    )

    # Add datasets
    await store.add_datasets(datasets)

    # Search
    results = await store.search("climate change rainfall")
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Callable

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import UpdateOne

from etl.models.dataset import DatasetMetadata
from .base import EmbeddingService


@dataclass
class SearchResult:
    """A semantic search result."""
    dataset_id: str
    title: str
    abstract: str
    score: float  # Similarity score (0-1, higher is better)
    keywords: list[str] = field(default_factory=list)


@dataclass
class IndexingResult:
    """Result of indexing datasets."""
    successful: list[str] = field(default_factory=list)  # Dataset IDs
    failed: list[tuple[str, str]] = field(default_factory=list)  # (ID, error)

    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None

    api_calls: int = 0

    @property
    def total(self) -> int:
        return len(self.successful) + len(self.failed)

    @property
    def success_rate(self) -> float:
        return len(self.successful) / self.total if self.total > 0 else 0.0

    def summary(self) -> str:
        duration = (self.completed_at or datetime.utcnow()) - self.started_at
        return (
            f"Indexed {len(self.successful)}/{self.total} datasets "
            f"({self.success_rate:.1%}) in {duration.total_seconds():.1f}s"
        )


# Progress callback: (dataset_id, current, total) -> None
ProgressCallback = Callable[[str, int, int], None]


class VectorStore:
    """
    Vector store using MongoDB Atlas vector search.

    Stores embeddings as a field (`embedding: list[float]`) on the
    dataset document itself. Uses $vectorSearch aggregation stage
    for similarity search (requires Atlas Vector Search index).

    Example:
        store = VectorStore(
            embedding_service=SentenceTransformerService(),
            collection=conn.datasets,
        )

        result = await store.add_datasets(datasets)
        print(result.summary())

        results = await store.search("drought conditions", limit=5)
        for r in results:
            print(f"{r.score:.3f} {r.title}")
    """

    VECTOR_INDEX_NAME = "vector_index"

    def __init__(
        self,
        embedding_service: EmbeddingService,
        collection: AsyncIOMotorCollection,
        batch_size: int = 32,
    ):
        self.embedding_service = embedding_service
        self._collection = collection
        self.batch_size = batch_size

    # =========================================================================
    # Indexing
    # =========================================================================

    async def add_datasets(
        self,
        datasets: list[DatasetMetadata],
        skip_existing: bool = True,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> IndexingResult:
        """
        Compute embeddings and store them on dataset documents.

        Args:
            datasets: Datasets to index
            skip_existing: Skip datasets that already have embeddings
            progress_callback: Optional progress callback
        """
        result = IndexingResult()

        if skip_existing:
            existing = set(await self.get_indexed_ids())
            datasets = [d for d in datasets if d.identifier not in existing]

        if not datasets:
            result.completed_at = datetime.utcnow()
            return result

        total = len(datasets)

        for i in range(0, len(datasets), self.batch_size):
            batch = datasets[i:i + self.batch_size]

            try:
                texts = [self._format_text(d) for d in batch]
                embeddings = await self.embedding_service.embed_batch(texts)
                result.api_calls += 1

                operations = [
                    UpdateOne(
                        {"_id": d.identifier},
                        {"$set": {"embedding": emb}},
                    )
                    for d, emb in zip(batch, embeddings)
                ]
                await self._collection.bulk_write(operations)

                for d in batch:
                    result.successful.append(d.identifier)
                    if progress_callback:
                        progress_callback(
                            d.identifier,
                            len(result.successful) + len(result.failed),
                            total,
                        )

            except Exception as e:
                for d in batch:
                    result.failed.append((d.identifier, str(e)))

        result.completed_at = datetime.utcnow()
        return result

    # =========================================================================
    # Search
    # =========================================================================

    async def search(
        self,
        query: str,
        limit: int = 10,
        min_score: float = 0.0,
    ) -> list[SearchResult]:
        """
        Semantic search using MongoDB Atlas $vectorSearch.

        Args:
            query: Search query
            limit: Maximum results
            min_score: Minimum similarity (0-1)
        """
        query_embedding = await self.embedding_service.embed_query(query)

        pipeline = [
            {
                "$vectorSearch": {
                    "index": self.VECTOR_INDEX_NAME,
                    "path": "embedding",
                    "queryVector": query_embedding,
                    "numCandidates": limit * 10,
                    "limit": limit,
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "title": 1,
                    "abstract": 1,
                    "keywords": 1,
                    "score": {"$meta": "vectorSearchScore"},
                }
            },
        ]

        results = []
        async for doc in self._collection.aggregate(pipeline):
            score = doc.get("score", 0)
            if score < min_score:
                continue

            results.append(SearchResult(
                dataset_id=str(doc["_id"]),
                title=doc.get("title", ""),
                abstract=doc.get("abstract", ""),
                score=score,
                keywords=doc.get("keywords", []) or [],
            ))

        return results

    # =========================================================================
    # Management
    # =========================================================================

    async def get_stats(self) -> dict:
        """Get store statistics."""
        total = await self._collection.count_documents({})
        indexed = await self._collection.count_documents(
            {"embedding": {"$exists": True}}
        )
        return {
            "total_documents": indexed,
            "total_datasets": total,
            "embedding_model": self.embedding_service.model_name,
            "embedding_dimensions": self.embedding_service.dimensions,
        }

    async def get_indexed_ids(self) -> list[str]:
        """Get all dataset IDs that have embeddings."""
        docs = await self._collection.find(
            {"embedding": {"$exists": True}},
            {"_id": 1},
        ).to_list(length=None)
        return [str(doc["_id"]) for doc in docs]

    async def clear(self) -> int:
        """Remove all embeddings. Returns count cleared."""
        result = await self._collection.update_many(
            {"embedding": {"$exists": True}},
            {"$unset": {"embedding": ""}},
        )
        return result.modified_count

    # =========================================================================
    # Internal
    # =========================================================================

    def _format_text(self, dataset: DatasetMetadata) -> str:
        """Format dataset for embedding."""
        parts = []
        if dataset.title:
            parts.append(dataset.title)
        if dataset.abstract:
            parts.append(dataset.abstract)
        return "\n\n".join(parts)


# =============================================================================
# Console Progress
# =============================================================================

def create_indexing_progress() -> ProgressCallback:
    """Create console progress callback for indexing."""
    last_len = 0

    def callback(dataset_id: str, current: int, total: int) -> None:
        nonlocal last_len

        pct = current / total * 100 if total > 0 else 0
        bar_width = 30
        filled = int(bar_width * current / total)
        bar = "\u2588" * filled + "\u2591" * (bar_width - filled)

        line = f"\r\U0001f9e0 [{bar}] {pct:5.1f}% ({current}/{total}) {dataset_id[:8]}..."

        padding = " " * max(0, last_len - len(line))
        print(line + padding, end="", flush=True)
        last_len = len(line)

        if current == total:
            print()

    return callback
