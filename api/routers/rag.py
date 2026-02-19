"""
RAG (Retrieval Augmented Generation) router.

Full RAG pipeline: vector retrieval -> context building -> LLM generation.
Falls back to context-only response when Ollama is unavailable.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from api.auth.dependencies import get_current_user
from api.dependencies import get_datasets_collection, get_embedding_model
from etl.rag.pipeline import RAGPipeline

router = APIRouter(tags=["rag"])


class RAGRequest(BaseModel):
    """Request body for RAG endpoint."""

    question: str = Field(..., min_length=3, description="The question to answer")
    top_k: int = Field(5, ge=1, le=20, description="Number of context documents")
    use_llm: bool = Field(True, description="Whether to use LLM for answer generation")


class RAGSource(BaseModel):
    id: str
    title: str
    relevance_score: float


class RAGResponse(BaseModel):
    question: str
    answer: str
    sources: List[RAGSource]
    generated: bool
    model: Optional[str]


@router.post("/rag", response_model=RAGResponse)
async def rag_query(
    body: RAGRequest,
    current_user=Depends(get_current_user),
    collection=Depends(get_datasets_collection),
    model=Depends(get_embedding_model),
):
    """
    Retrieval Augmented Generation endpoint.

    Retrieves relevant datasets via vector search, builds context,
    and generates an answer using Ollama (with fallback to context-only).
    Context is filtered by the user's access level. PII is redacted from answers.
    """
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Embedding model not available. Cannot perform vector search.",
        )

    user_role = current_user.get("role") if current_user else None

    pipeline = RAGPipeline(collection, model)
    result = await pipeline.query(
        question=body.question,
        top_k=body.top_k,
        use_llm=body.use_llm,
        user_role=user_role,
    )
    return result
