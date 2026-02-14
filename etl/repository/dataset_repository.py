"""
Dataset repository implementation - async MongoDB version.

All methods are async. Datasets are stored as flat documents
using Pydantic model_dump() — no ORM, no converters needed.
"""

from __future__ import annotations

from typing import Optional

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ReplaceOne

from etl.models.dataset import DatasetMetadata
from .base import BulkOperationResult, PagedResult


class DatasetRepository:
    """
    Async MongoDB repository for dataset metadata.

    Stores datasets as flat BSON documents. Uses the dataset
    `identifier` as the MongoDB `_id` field.
    """

    def __init__(self, collection: AsyncIOMotorCollection):
        self._collection = collection

    # =========================================================================
    # Core CRUD Operations
    # =========================================================================

    async def get(self, identifier: str, **kwargs) -> Optional[DatasetMetadata]:
        """Retrieve a dataset by identifier."""
        doc = await self._collection.find_one({"_id": identifier})
        if doc is None:
            return None
        return self._doc_to_domain(doc)

    async def get_all(self) -> list[DatasetMetadata]:
        """Retrieve all datasets."""
        docs = await self._collection.find(
            {}, {"embedding": 0}
        ).to_list(length=None)
        return [self._doc_to_domain(d) for d in docs]

    async def get_all_for_embedding(self) -> list[DatasetMetadata]:
        """
        Get datasets optimized for embedding generation.

        Only loads fields needed for embeddings: identifier, title, abstract.
        """
        projection = {"_id": 1, "title": 1, "abstract": 1}
        docs = await self._collection.find({}, projection).to_list(length=None)
        return [
            DatasetMetadata(
                identifier=str(doc["_id"]),
                title=doc.get("title", ""),
                abstract=doc.get("abstract", ""),
                keywords=[],
            )
            for doc in docs
        ]

    async def get_paged(
        self,
        page: int = 1,
        page_size: int = 20,
        **kwargs,
    ) -> PagedResult[DatasetMetadata]:
        """Retrieve datasets with pagination."""
        total = await self._collection.count_documents({})
        skip = (page - 1) * page_size

        docs = await (
            self._collection.find({}, {"embedding": 0})
            .sort("title", 1)
            .skip(skip)
            .limit(page_size)
            .to_list(length=page_size)
        )
        items = [self._doc_to_domain(d) for d in docs]

        return PagedResult(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )

    async def save(self, entity: DatasetMetadata) -> str:
        """Save a dataset (insert or update)."""
        doc = self._domain_to_doc(entity)
        await self._collection.replace_one(
            {"_id": entity.identifier},
            doc,
            upsert=True,
        )
        return entity.identifier

    async def delete(self, identifier: str) -> bool:
        """Delete a dataset by identifier."""
        result = await self._collection.delete_one({"_id": identifier})
        return result.deleted_count > 0

    async def exists(self, identifier: str) -> bool:
        """Check if a dataset exists."""
        count = await self._collection.count_documents(
            {"_id": identifier}, limit=1
        )
        return count > 0

    async def count(self) -> int:
        """Count total datasets."""
        return await self._collection.count_documents({})

    # =========================================================================
    # Search Operations
    # =========================================================================

    async def search(self, query: str, limit: int = 100) -> list[DatasetMetadata]:
        """Search datasets by text query using regex on title/abstract."""
        import re
        pattern = re.escape(query)
        filter_query = {
            "$or": [
                {"title": {"$regex": pattern, "$options": "i"}},
                {"abstract": {"$regex": pattern, "$options": "i"}},
            ]
        }
        docs = await (
            self._collection.find(filter_query, {"embedding": 0})
            .limit(limit)
            .to_list(length=limit)
        )
        return [self._doc_to_domain(d) for d in docs]

    async def get_all_identifiers(self) -> list[str]:
        """Get all dataset identifiers (efficient)."""
        docs = await self._collection.find(
            {}, {"_id": 1}
        ).to_list(length=None)
        return [str(doc["_id"]) for doc in docs]

    # =========================================================================
    # Bulk Operations
    # =========================================================================

    async def save_many(self, entities: list[DatasetMetadata]) -> BulkOperationResult:
        """Save multiple datasets using bulk_write with upserts."""
        result = BulkOperationResult()

        if not entities:
            return result

        operations = []
        for entity in entities:
            try:
                doc = self._domain_to_doc(entity)
                operations.append(
                    ReplaceOne(
                        {"_id": entity.identifier},
                        doc,
                        upsert=True,
                    )
                )
                result.add_success(entity.identifier)
            except Exception as e:
                result.add_failure(entity.identifier, str(e))

        if operations:
            try:
                await self._collection.bulk_write(operations, ordered=False)
            except Exception as e:
                # If bulk write fails, mark all as failed
                result.succeeded.clear()
                for entity in entities:
                    result.add_failure(entity.identifier, str(e))

        return result

    async def clear_all(self) -> int:
        """Delete all datasets."""
        count = await self._collection.count_documents({})
        await self._collection.delete_many({})
        return count

    # =========================================================================
    # Conversion Helpers
    # =========================================================================

    @staticmethod
    def _domain_to_doc(entity: DatasetMetadata) -> dict:
        """Convert Pydantic model to MongoDB document."""
        doc = entity.model_dump(mode="json", exclude_none=True)
        # Use identifier as _id
        doc["_id"] = doc.pop("identifier")
        # Remove raw_document from storage (large, not needed for search)
        doc.pop("raw_document", None)
        doc.pop("source_format", None)
        return doc

    @staticmethod
    def _doc_to_domain(doc: dict) -> DatasetMetadata:
        """Convert MongoDB document to Pydantic model."""
        data = dict(doc)
        # Map _id back to identifier
        data["identifier"] = str(data.pop("_id"))
        # Remove embedding field (not part of domain model)
        data.pop("embedding", None)
        return DatasetMetadata.model_validate(data)
