"""Admin router — upload, pending CRUD, approve/reject, bulk import."""

import csv
import io
import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

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


class BulkUploadResponse(BaseModel):
    success: bool
    message: str
    datasets_created: int = 0
    errors: List[str] = []


class UploadedDatasetItem(BaseModel):
    id: str
    title: str
    source: str
    access_level: str
    uploaded_at: datetime


class UploadedListResponse(BaseModel):
    items: List[UploadedDatasetItem]
    total: int


# =============================================================================
# File parsing constants + helpers
# =============================================================================

ALLOWED_EXTENSIONS = {".json", ".csv", ".xlsx", ".xls", ".pdf"}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


def get_file_extension(filename: str) -> str:
    """Return the lowercase extension if it is in ALLOWED_EXTENSIONS, else ''."""
    if not filename:
        return ""
    lower = filename.lower()
    for ext in ALLOWED_EXTENSIONS:
        if lower.endswith(ext):
            return ext
    return ""


def parse_json_file(content: bytes) -> List[dict]:
    """Parse JSON file — single object or array of objects."""
    try:
        data = json.loads(content.decode("utf-8"))
        if isinstance(data, dict):
            return [data]
        elif isinstance(data, list):
            return data
        else:
            raise ValueError("JSON must be an object or array of objects")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")


def parse_csv_file(content: bytes) -> List[dict]:
    """Parse CSV file with header row."""
    try:
        text = content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        datasets = []
        for row in reader:
            # Convert comma-separated keywords to list
            if "keywords" in row and isinstance(row["keywords"], str):
                row["keywords"] = [k.strip() for k in row["keywords"].split(",") if k.strip()]
            # Parse bounding_box if JSON string
            if "bounding_box" in row and isinstance(row["bounding_box"], str):
                try:
                    row["bounding_box"] = json.loads(row["bounding_box"])
                except Exception:
                    row.pop("bounding_box", None)
            # Parse temporal_extent if JSON string
            if "temporal_extent" in row and isinstance(row["temporal_extent"], str):
                try:
                    row["temporal_extent"] = json.loads(row["temporal_extent"])
                except Exception:
                    row.pop("temporal_extent", None)
            # Remove empty string values
            row = {k: v for k, v in row.items() if v}
            if row.get("title"):
                datasets.append(row)
        return datasets
    except Exception as e:
        raise ValueError(f"Invalid CSV: {e}")


def parse_xlsx_file(content: bytes) -> List[dict]:
    """Parse Excel (.xlsx/.xls) file."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 2:
            raise ValueError("Excel file must have a header row and at least one data row")

        headers = [
            str(h).strip().lower() if h else f"col_{i}"
            for i, h in enumerate(rows[0])
        ]
        datasets = []
        for row in rows[1:]:
            if not any(row):
                continue
            data: dict = {}
            for i, value in enumerate(row):
                if i < len(headers) and value is not None:
                    header = headers[i]
                    if header == "keywords" and isinstance(value, str):
                        data[header] = [k.strip() for k in value.split(",") if k.strip()]
                    else:
                        data[header] = value
            if data.get("title"):
                datasets.append(data)
        return datasets
    except ImportError:
        raise ValueError("Excel support requires the openpyxl package")
    except Exception as e:
        raise ValueError(f"Invalid Excel file: {e}")


def parse_pdf_file(content: bytes) -> tuple[dict, str]:
    """Extract text from a PDF. Returns (metadata_dict, full_text)."""
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages_text = [page.extract_text() or "" for page in pdf.pages]
            full_text = "\n\n".join(pages_text).strip()

        if not full_text:
            raise ValueError("No text could be extracted from PDF")

        return {"extracted_text": full_text}, full_text
    except Exception as e:
        raise ValueError(f"Failed to read PDF: {e}")


# =============================================================================
# Internal helpers
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
# Endpoints — PDF upload (pending workflow)
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


# =============================================================================
# Endpoints — Bulk import (JSON / CSV / XLSX / PDF)
# =============================================================================

@router.post("/bulk-upload", response_model=BulkUploadResponse, status_code=status.HTTP_201_CREATED)
async def bulk_upload(
    file: UploadFile,
    source: str = "manual_upload",
    user: AdminUser = None,
):
    """
    Bulk upload datasets from JSON, CSV, XLSX, or PDF files.

    Records go directly to the datasets collection (no pending review step).
    For PDFs that need manual metadata review, use /admin/upload instead.

    File format notes:
    - JSON: single object ``{}`` or array ``[{}, ...]``  — each object becomes a dataset.
    - CSV: header row required; ``keywords`` column may be comma-separated.
    - XLSX/XLS: first row is header; ``keywords`` column may be comma-separated.
    - PDF: a single dataset entry is created from the extracted text.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    ext = get_file_extension(file.filename)
    if not ext:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )

    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File too large (max 10 MB)")

    # Parse based on extension
    try:
        if ext == ".json":
            datasets = parse_json_file(content)
        elif ext == ".csv":
            datasets = parse_csv_file(content)
        elif ext in {".xlsx", ".xls"}:
            datasets = parse_xlsx_file(content)
        else:  # .pdf
            _, text = parse_pdf_file(content)
            stem = file.filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " ").title()
            datasets = [{
                "title": stem,
                "abstract": text[:500] if text else "Uploaded PDF document",
                "keywords": ["pdf", "document"],
                "lineage": f"Extracted from uploaded PDF: {file.filename}",
            }]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not datasets:
        raise HTTPException(status_code=400, detail="No valid datasets found in file")

    conn = get_mongo_connection()
    embedding_service = get_embedding_service()

    imported = 0
    errors: List[str] = []

    for i, data in enumerate(datasets):
        try:
            if not data.get("title"):
                errors.append(f"Row {i + 1}: missing required field 'title'")
                continue

            identifier = str(data.get("identifier") or f"{source}-{uuid.uuid4().hex[:8]}")

            access_level = data.get("access_level", "public")
            if access_level not in {"public", "restricted", "admin_only"}:
                access_level = "public"

            doc: dict = {
                "identifier": identifier,
                "title": str(data["title"]),
                "abstract": str(data.get("abstract", "")),
                "keywords": data.get("keywords", []),
                "topic_categories": data.get("topic_categories", []),
                "lineage": data.get("lineage") or None,
                "bounding_box": data.get("bounding_box"),
                "temporal_extent": data.get("temporal_extent"),
                "access_level": access_level,
                "source": source,
                "uploaded_by": user["sub"] if user else "system",
                "uploaded_at": datetime.now(timezone.utc),
            }

            # Generate embedding (non-fatal)
            if embedding_service:
                try:
                    embed_text = f"{doc['title']} {doc.get('abstract', '')}"
                    embedding = embedding_service.embed(embed_text)
                    doc["embedding"] = embedding.tolist()
                except Exception:
                    pass

            # ISO 19115 compliance (non-fatal)
            try:
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
                doc["iso_compliance"] = compliance
            except Exception:
                pass

            # Strip None values
            doc = {k: v for k, v in doc.items() if v is not None}

            await conn.datasets.update_one(
                {"identifier": identifier},
                {"$set": doc},
                upsert=True,
            )
            imported += 1

        except Exception as e:
            errors.append(f"Row {i + 1}: {e}")

    return BulkUploadResponse(
        success=imported > 0,
        message=f"Imported {imported} of {len(datasets)} dataset(s)",
        datasets_created=imported,
        errors=errors[:10],  # cap at 10 to keep response size reasonable
    )


@router.get("/uploaded", response_model=UploadedListResponse)
async def list_uploaded(user: AdminUser):
    """List manually uploaded datasets (source != 'ceh')."""
    conn = get_mongo_connection()

    items: List[UploadedDatasetItem] = []
    async for doc in conn.datasets.find(
        {"source": {"$nin": ["ceh", None]}},
        {"_id": 0, "identifier": 1, "title": 1, "source": 1, "access_level": 1, "uploaded_at": 1},
    ).sort("uploaded_at", -1).limit(100):
        items.append(UploadedDatasetItem(
            id=doc.get("identifier", ""),
            title=doc.get("title", "Untitled"),
            source=doc.get("source", "unknown"),
            access_level=doc.get("access_level", "public"),
            uploaded_at=doc.get("uploaded_at", datetime.now(timezone.utc)),
        ))

    return UploadedListResponse(items=items, total=len(items))


@router.delete("/dataset/{identifier}")
async def delete_dataset(identifier: str, user: AdminUser):
    """Delete a dataset from the catalogue by identifier."""
    conn = get_mongo_connection()
    result = await conn.datasets.delete_one({"identifier": identifier})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return {"success": True, "message": f"Deleted dataset: {identifier}"}


# =============================================================================
# Endpoints — Pending workflow
# =============================================================================

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
