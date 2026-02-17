"""Admin router — upload, pending CRUD, approve/reject."""

import uuid
from datetime import datetime, timezone
from typing import Optional

import pdfplumber
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, UploadFile, status
from motor.motor_asyncio import AsyncIOMotorGridFSBucket
from pydantic import BaseModel

from api.auth.dependencies import AdminUser
from api.dependencies import get_dataset_repository, get_embedding_service, get_mongo_connection
from api.schemas.responses import ComplianceInfo
from etl.extraction.metadata_extractor import MetadataExtractor
from etl.models.dataset import DatasetMetadata
from etl.validation.iso_compliance import check_compliance

router = APIRouter(prefix="/admin", tags=["admin"])
extractor = MetadataExtractor()


# =============================================================================
# Schemas
# =============================================================================

class PendingItem(BaseModel):
    id: str
    title: Optional[str] = None
    abstract: Optional[str] = None
    keywords: list[str] = []
    topic_categories: list[str] = []
    lineage: Optional[str] = None
    filename: str
    uploaded_by: str
    uploaded_at: datetime
    iso_compliance: Optional[ComplianceInfo] = None


class PendingListResponse(BaseModel):
    items: list[PendingItem]
    total: int
    page: int
    page_size: int


class PendingUpdateRequest(BaseModel):
    title: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[list[str]] = None
    topic_categories: Optional[list[str]] = None
    lineage: Optional[str] = None


# =============================================================================
# Helpers
# =============================================================================

def _pending_doc_to_item(doc: dict) -> PendingItem:
    return PendingItem(
        id=str(doc["_id"]),
        title=doc.get("title"),
        abstract=doc.get("abstract"),
        keywords=doc.get("keywords", []),
        topic_categories=doc.get("topic_categories", []),
        lineage=doc.get("lineage"),
        filename=doc.get("filename", ""),
        uploaded_by=doc.get("uploaded_by", ""),
        uploaded_at=doc.get("uploaded_at", datetime.now(timezone.utc)),
    )


# =============================================================================
# Endpoints
# =============================================================================

@router.post("/upload", response_model=PendingItem, status_code=status.HTTP_201_CREATED)
async def upload_pdf(file: UploadFile, user: AdminUser):
    """Upload a PDF, extract metadata, store in pending collection."""
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    # Extract text from PDF
    import io
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages_text = [page.extract_text() or "" for page in pdf.pages]
            full_text = "\n\n".join(pages_text).strip()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read PDF: {e}")

    if not full_text:
        raise HTTPException(status_code=400, detail="No text could be extracted from PDF")

    # Extract metadata
    metadata = await extractor.extract(full_text)

    # Store PDF in GridFS
    conn = get_mongo_connection()
    fs = AsyncIOMotorGridFSBucket(conn.db, bucket_name="documents")
    grid_id = await fs.upload_from_stream(file.filename, content)

    # Save to pending collection
    doc = {
        "title": metadata.get("title"),
        "abstract": metadata.get("abstract"),
        "keywords": metadata.get("keywords", []),
        "topic_categories": metadata.get("topic_categories", []),
        "lineage": None,
        "filename": file.filename,
        "gridfs_id": grid_id,
        "extracted_text": full_text[:10000],  # Keep first 10k chars for reference
        "uploaded_by": user["sub"],
        "uploaded_at": datetime.now(timezone.utc),
    }
    result = await conn.pending.insert_one(doc)
    doc["_id"] = result.inserted_id

    return _pending_doc_to_item(doc)


@router.get("/pending", response_model=PendingListResponse)
async def list_pending(
    user: AdminUser,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List pending datasets with pagination."""
    conn = get_mongo_connection()
    total = await conn.pending.count_documents({})
    skip = (page - 1) * page_size

    docs = await (
        conn.pending.find()
        .sort("uploaded_at", -1)
        .skip(skip)
        .limit(page_size)
        .to_list(length=page_size)
    )

    return PendingListResponse(
        items=[_pending_doc_to_item(d) for d in docs],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.put("/pending/{pending_id}", response_model=PendingItem)
async def update_pending(pending_id: str, body: PendingUpdateRequest, user: AdminUser):
    """Edit metadata fields on a pending dataset."""
    conn = get_mongo_connection()

    try:
        oid = ObjectId(pending_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    update_fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    result = await conn.pending.find_one_and_update(
        {"_id": oid},
        {"$set": update_fields},
        return_document=True,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Pending dataset not found")

    return _pending_doc_to_item(result)


@router.post("/approve/{pending_id}")
async def approve_pending(pending_id: str, user: AdminUser):
    """Approve a pending dataset: move to datasets collection."""
    conn = get_mongo_connection()

    try:
        oid = ObjectId(pending_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    doc = await conn.pending.find_one({"_id": oid})
    if doc is None:
        raise HTTPException(status_code=404, detail="Pending dataset not found")

    # Create DatasetMetadata
    identifier = str(uuid.uuid4())
    dataset = DatasetMetadata(
        identifier=identifier,
        title=doc.get("title") or "Untitled",
        abstract=doc.get("abstract"),
        keywords=doc.get("keywords", []),
        topic_categories=doc.get("topic_categories", []),
        lineage=doc.get("lineage"),
    )

    # Save to datasets
    repo = get_dataset_repository()
    await repo.save(dataset)

    # Run ISO 19115 compliance check
    compliance = check_compliance({
        "identifier": identifier,
        "title": doc.get("title"),
        "abstract": doc.get("abstract"),
        "keywords": doc.get("keywords", []),
        "topic_categories": doc.get("topic_categories", []),
        "lineage": doc.get("lineage"),
        "bounding_box": doc.get("bounding_box"),
        "temporal_extent": doc.get("temporal_extent"),
    })

    await conn.datasets.update_one(
        {"identifier": identifier},
        {"$set": {"iso_compliance": compliance}},
    )

    # Generate embedding if available
    embedding_service = get_embedding_service()
    if embedding_service:
        try:
            embedding = embedding_service.embed(dataset.search_text)
            await conn.datasets.update_one(
                {"identifier": identifier},
                {"$set": {"embedding": embedding.tolist()}},
            )
        except Exception:
            pass  # Non-fatal: dataset saved without embedding

    # Remove from pending
    await conn.pending.delete_one({"_id": oid})

    return {
        "message": "Dataset approved",
        "identifier": identifier,
        "title": dataset.title,
        "iso_compliance": compliance,
    }


@router.delete("/reject/{pending_id}")
async def reject_pending(pending_id: str, user: AdminUser):
    """Reject and delete a pending dataset + its GridFS file."""
    conn = get_mongo_connection()

    try:
        oid = ObjectId(pending_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    doc = await conn.pending.find_one({"_id": oid})
    if doc is None:
        raise HTTPException(status_code=404, detail="Pending dataset not found")

    # Delete GridFS file
    if doc.get("gridfs_id"):
        try:
            fs = AsyncIOMotorGridFSBucket(conn.db, bucket_name="documents")
            await fs.delete(doc["gridfs_id"])
        except Exception:
            pass  # Non-fatal

    # Delete pending doc
    await conn.pending.delete_one({"_id": oid})

    return {"message": "Pending dataset rejected and deleted"}


@router.get("/pending/{pending_id}/compliance", response_model=ComplianceInfo)
async def check_pending_compliance(pending_id: str, user: AdminUser):
    """Preview ISO 19115 compliance for a pending dataset."""
    conn = get_mongo_connection()

    try:
        oid = ObjectId(pending_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    doc = await conn.pending.find_one({"_id": oid})
    if doc is None:
        raise HTTPException(status_code=404, detail="Pending dataset not found")

    result = check_compliance({
        "identifier": str(doc["_id"]),
        "title": doc.get("title"),
        "abstract": doc.get("abstract"),
        "keywords": doc.get("keywords", []),
        "topic_categories": doc.get("topic_categories", []),
        "lineage": doc.get("lineage"),
        "bounding_box": doc.get("bounding_box"),
        "temporal_extent": doc.get("temporal_extent"),
    })

    return ComplianceInfo(**result)
