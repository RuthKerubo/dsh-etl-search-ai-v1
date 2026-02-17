"""Async MongoDB user repository."""

from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorCollection


class UserRepositoryMongo:
    """
    Async user repository using MongoDB.

    Uses email as _id (follows identifier-as-_id pattern).
    """

    def __init__(self, collection: AsyncIOMotorCollection):
        self._collection = collection

    async def get_by_email(self, email: str) -> Optional[dict]:
        return await self._collection.find_one({"_id": email})

    async def exists(self, email: str) -> bool:
        count = await self._collection.count_documents({"_id": email}, limit=1)
        return count > 0

    async def create(self, email: str, hashed_password: str, role: str) -> dict:
        doc = {
            "_id": email,
            "email": email,
            "hashed_password": hashed_password,
            "role": role,
            "created_at": datetime.now(timezone.utc),
        }
        await self._collection.insert_one(doc)
        return doc
