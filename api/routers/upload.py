"""
Upload router - accepts PDF, CSV, or JSON files.

Extracts text, generates embeddings, stores in MongoDB.
"""

import csv
import io
import json
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pymongo import UpdateOne

from api.dependencies import (
    get_dataset_repository,
    get_embedding_service,
    get_mongo_connection,
)
from api.schemas.responses import UploadResponse
from etl.models.dataset import DatasetMetadata

router = APIRouter(tags=["upload"])

# Max file size: 10 MB
MAX_FILE_SIZE = 10 * 1024 * 1024

ALLOWED_CONTENT_TYPES = {
    "application/pdf",
    "text/csv",
    "application/json",
    "application/octet-stream",  # fallback for some clients
}

ALLOWED_EXTENSIONS = {".pdf", ".csv", ".json"}


def _extract_extension(filename: str) -> str:
    """Get lowercase file extension."""
    if "." in filename:
        return "." + filename.rsplit(".", 1)[-1].lower()
    return ""


async def _extract_text_pdf(content: bytes) -> tuple[str, str]:
    """Extract text from PDF. Returns (title, body)."""
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(content))
    pages = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            pages.append(text.strip())

    if not pages:
        raise HTTPException(status_code=422, detail="Could not extract text from PDF")

    full_text = "\n\n".join(pages)
    # Use first line as title, rest as abstract
    lines = full_text.strip().split("\n", 1)
    title = lines[0].strip()[:200]
    body = lines[1].strip() if len(lines) > 1 else title
    return title, body


def _extract_text_csv(content: bytes) -> tuple[str, str]:
    """Extract text from CSV. Returns (title, body)."""
    text = content.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)

    if not rows:
        raise HTTPException(status_code=422, detail="CSV file is empty")

    headers = rows[0]
    title = f"CSV Dataset ({len(rows) - 1} rows, {len(headers)} columns)"
    # Build a summary of headers + first few rows
    parts = [f"Columns: {', '.join(headers)}"]
    for row in rows[1:6]:  # First 5 data rows
        parts.append(" | ".join(row))
    if len(rows) > 6:
        parts.append(f"... and {len(rows) - 6} more rows")

    body = "\n".join(parts)
    return title, body


def _extract_text_json(content: bytes) -> tuple[str, str]:
    """Extract text from JSON. Returns (title, body)."""
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {e}")

    if isinstance(data, dict):
        title = data.get("title", data.get("name", "JSON Document"))
        abstract = data.get("abstract", data.get("description", ""))
        if not abstract:
            abstract = json.dumps(data, indent=2)[:2000]
        return str(title)[:200], str(abstract)

    # Array of objects
    if isinstance(data, list):
        title = f"JSON Dataset ({len(data)} records)"
        body = json.dumps(data[:5], indent=2)[:2000]
        if len(data) > 5:
            body += f"\n... and {len(data) - 5} more records"
        return title, body

    return "JSON Document", str(data)[:2000]


@router.post("/upload", response_model=UploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    repo=Depends(get_dataset_repository),
    embedding_service=Depends(get_embedding_service),
    mongo_conn=Depends(get_mongo_connection),
):
    """
    Upload a PDF, CSV, or JSON file.

    Extracts text content, generates embeddings, and stores in MongoDB.
    """
    # Validate file extension
    ext = _extract_extension(file.filename or "")
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{ext}'. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        )

    # Read content
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413, detail=f"File too large. Max size: {MAX_FILE_SIZE // (1024*1024)} MB"
        )

    if not content:
        raise HTTPException(status_code=400, detail="File is empty")

    # Extract text based on file type
    if ext == ".pdf":
        title, abstract = await _extract_text_pdf(content)
    elif ext == ".csv":
        title, abstract = _extract_text_csv(content)
    elif ext == ".json":
        title, abstract = _extract_text_json(content)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")

    # Create dataset
    identifier = f"upload-{uuid.uuid4().hex[:12]}"
    dataset = DatasetMetadata(
        identifier=identifier,
        title=title,
        abstract=abstract,
        keywords=[f"uploaded:{ext.lstrip('.')}"],
    )

    # Store in MongoDB
    await repo.save(dataset)

    # Generate and store embedding
    embedded = False
    if embedding_service:
        try:
            text_for_embedding = f"{title}\n\n{abstract}"
            embedding = await embedding_service.embed_query(text_for_embedding)
            await mongo_conn.datasets.update_one(
                {"_id": identifier},
                {"$set": {"embedding": embedding}},
            )
            embedded = True
        except Exception as e:
            # Store succeeded, embedding failed - not fatal
            print(f"Warning: embedding failed for {identifier}: {e}")

    return UploadResponse(
        identifier=identifier,
        title=title,
        abstract=abstract[:500],
        keywords=dataset.keywords,
        embedded=embedded,
        message=f"Document uploaded and {'embedded' if embedded else 'stored (embedding unavailable)'}",
    )
