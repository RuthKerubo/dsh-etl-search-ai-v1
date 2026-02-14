"""
DSH ETL Search AI - FastAPI Application.

Dataset search and discovery API with hybrid search capabilities.

Usage:
    uvicorn api.main:app --reload

    # Or
    python -m api.main
"""
from dotenv import load_dotenv
load_dotenv()

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.dependencies import init_dependencies, shutdown_dependencies, get_service_status
from api.routers import health, datasets, search, upload, rag


# =============================================================================
# Lifespan Management
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.

    Initializes dependencies on startup, cleans up on shutdown.
    """
    # Startup
    print("Starting DSH ETL Search AI...")
    await init_dependencies()

    status = await get_service_status()

    db_ok = status['database']
    vs_ok = status['vector_store']
    emb_ok = status['embedding_service']
    print(f"   Database: {'ok' if db_ok else 'FAILED'} ({status['dataset_count']} datasets)")
    print(f"   Vector store: {'ok' if vs_ok else 'not available'} ({status['indexed_count']} indexed)")
    print(f"   Embeddings: {'ok' if emb_ok else 'not available (keyword-only mode)'}")
    print()

    yield

    # Shutdown
    print("Shutting down...")
    await shutdown_dependencies()


# =============================================================================
# Application Setup
# =============================================================================

app = FastAPI(
    title="DSH ETL Search AI",
    description="""
Dataset search and discovery API for CEH Environmental Information Data Centre.

## Features
- **Hybrid Search**: Combines semantic (vector) and keyword search
- **Document Upload**: Upload PDF, CSV, or JSON files with automatic embedding
- **RAG**: Retrieval Augmented Generation - ask questions, get context-based answers
- **Automatic Fallback**: Works without embeddings (keyword-only mode)
- **Pagination**: All list endpoints support pagination

## Search Modes
The `/search` endpoint automatically selects the best mode:
- **hybrid**: Semantic + keyword (when embeddings available)
- **keyword**: MongoDB regex matching (fallback)

You can force a mode with `?mode=keyword` or `?mode=semantic`.
    """,
    version="0.2.0",
    lifespan=lifespan,
)

# CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # Svelte dev server
        "http://localhost:5173",  # Vite dev server
        "http://localhost:5000",  # Alternative
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Routers
# =============================================================================

app.include_router(health.router)
app.include_router(datasets.router, prefix="/api")
app.include_router(search.router, prefix="/api")
app.include_router(upload.router, prefix="/api")
app.include_router(rag.router, prefix="/api")


# =============================================================================
# Root Endpoint
# =============================================================================

@app.get("/")
def root():
    """API root - redirects to docs."""
    return {
        "name": "DSH ETL Search AI",
        "docs": "/docs",
        "health": "/health",
    }


# =============================================================================
# Run with Python
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
