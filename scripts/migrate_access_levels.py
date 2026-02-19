"""
One-off migration: set access_level on all dataset documents.

Steps:
  1. Set access_level = "public" on all documents (safe default).
  2. Set access_level = "restricted" for datasets whose keywords or abstract
     contain sensitive terms (protected species, embargoed, sensitive location).
  3. Promote one example to "admin_only" for demonstration.

Run:
    python scripts/migrate_access_levels.py
"""

import asyncio
import os
import sys

# Allow importing from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

import motor.motor_asyncio


async def migrate():
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGODB_DB", "dsh_etl")
    collection_name = os.getenv("MONGODB_COLLECTION", "datasets")

    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # -------------------------------------------------------------------------
    # Step 1: Set all documents to "public" (idempotent)
    # -------------------------------------------------------------------------
    result = await collection.update_many(
        {},
        {"$set": {"access_level": "public"}},
    )
    print(f"Step 1: Set {result.modified_count} documents to access_level='public'")

    # -------------------------------------------------------------------------
    # Step 2: Set "restricted" for sensitive datasets
    # -------------------------------------------------------------------------
    sensitive_filter = {
        "$or": [
            {"keywords": {"$regex": "protected species", "$options": "i"}},
            {"abstract": {"$regex": "embargoed|sensitive location", "$options": "i"}},
            {"title": {"$regex": "confidential|restricted", "$options": "i"}},
        ]
    }
    result = await collection.update_many(
        sensitive_filter,
        {"$set": {"access_level": "restricted"}},
    )
    print(f"Step 2: Set {result.modified_count} documents to access_level='restricted'")

    # -------------------------------------------------------------------------
    # Step 3: Promote first restricted doc to "admin_only" as example
    # -------------------------------------------------------------------------
    restricted_doc = await collection.find_one({"access_level": "restricted"})
    if restricted_doc:
        await collection.update_one(
            {"_id": restricted_doc["_id"]},
            {"$set": {"access_level": "admin_only"}},
        )
        title = restricted_doc.get("title", str(restricted_doc["_id"]))
        print(f"Step 3: Promoted '{title[:60]}' to access_level='admin_only'")
    else:
        print("Step 3: No restricted documents found — skipping admin_only promotion")

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    public_count = await collection.count_documents({"access_level": "public"})
    restricted_count = await collection.count_documents({"access_level": "restricted"})
    admin_count = await collection.count_documents({"access_level": "admin_only"})

    print("\nMigration complete:")
    print(f"  public     : {public_count}")
    print(f"  restricted : {restricted_count}")
    print(f"  admin_only : {admin_count}")

    client.close()


if __name__ == "__main__":
    asyncio.run(migrate())
