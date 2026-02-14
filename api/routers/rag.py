"""
RAG (Retrieval Augmented Generation) router.

Accepts a question, retrieves top relevant documents via vector search,
returns context and a formatted answer. LLM integration can be added later.
"""

from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_vector_store
from api.schemas.responses import RAGContextDocument, RAGResponse

router = APIRouter(tags=["rag"])


class RAGRequest(BaseModel):
    """Request body for RAG endpoint."""
    question: str = Field(..., min_length=3, description="The question to answer")
    top_k: int = Field(5, ge=1, le=20, description="Number of context documents")


@router.post("/rag")
async def rag_query(
    body: RAGRequest,
    vector_store=Depends(get_vector_store),
) -> RAGResponse:
    """
    Retrieval Augmented Generation endpoint.

    Retrieves the top-k most relevant documents for the given question
    using vector search, then returns the context and a formatted answer.

    LLM integration for answer generation can be added later -
    currently returns a structured context-based response.
    """
    if vector_store is None:
        raise HTTPException(
            status_code=503,
            detail="Vector search is not available. Embedding service may not be configured.",
        )

    question = body.question
    top_k = body.top_k

    # Retrieve relevant documents via vector search
    results = await vector_store.search(query=question, limit=top_k)

    if not results:
        return RAGResponse(
            question=question,
            answer="No relevant documents found for your question.",
            context=[],
            total_context_docs=0,
        )

    # Build context documents
    context = [
        RAGContextDocument(
            identifier=r.dataset_id,
            title=r.title,
            abstract=r.abstract,
            score=r.score,
            keywords=r.keywords,
        )
        for r in results
    ]

    # Generate answer from context (template-based for now, LLM later)
    answer = _generate_answer(question, results)

    return RAGResponse(
        question=question,
        answer=answer,
        context=context,
        total_context_docs=len(context),
    )


def _generate_answer(question: str, results) -> str:
    """
    Generate an answer from the retrieved context.

    Currently uses template-based formatting. Replace with LLM call
    (e.g. Claude API) for natural language generation.
    """
    top = results[0]
    lines = [
        f"Based on {len(results)} relevant documents:\n",
        f"**Most relevant:** {top.title} (score: {top.score:.3f})\n",
        f"{top.abstract[:300]}{'...' if len(top.abstract) > 300 else ''}\n",
    ]

    if len(results) > 1:
        lines.append("\n**Other relevant documents:**")
        for r in results[1:]:
            lines.append(f"- {r.title} (score: {r.score:.3f})")

    return "\n".join(lines)
