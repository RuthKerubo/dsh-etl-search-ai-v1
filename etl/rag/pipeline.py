"""Main RAG Pipeline"""

from typing import Any, Dict, Optional

from .context_builder import build_context
from .generator import generate_answer
from .retriever import DatasetRetriever
from etl.guardrails import DataGuardrails, RAGGuardrails


class RAGPipeline:
    def __init__(self, collection, embedding_model):
        self.retriever = DatasetRetriever(collection, embedding_model)

    async def query(
        self,
        question: str,
        top_k: int = 5,
        min_relevance: float = 0.3,
        use_llm: bool = True,
        user_role: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Full RAG pipeline with access control and PII guardrails."""

        # 1. Retrieve
        retrieved = await self.retriever.retrieve(
            query=question,
            top_k=top_k,
            min_score=min_relevance,
        )

        # 2. Filter by access level
        retrieved = RAGGuardrails.filter_context_by_access(retrieved, user_role)

        if not retrieved:
            return {
                "question": question,
                "answer": "No relevant datasets found. Try different keywords.",
                "sources": [],
                "generated": False,
                "model": None,
            }

        # 3. Build context
        context = build_context(retrieved)

        # 4. Generate
        if use_llm:
            generation = await generate_answer(question, context)
        else:
            generation = {
                "answer": f"Found {len(retrieved)} relevant datasets:\n\n{context}",
                "model": "fallback",
                "generated": False,
            }

        # 5. Redact PII from answer
        validated = RAGGuardrails.validate_response(generation["answer"], user_role)
        generation["answer"] = validated["response"]

        return {
            "question": question,
            "answer": generation["answer"],
            "sources": [
                {
                    "id": d["id"],
                    "title": d["title"],
                    "relevance_score": d["relevance_score"],
                }
                for d in retrieved
            ],
            "generated": generation["generated"],
            "model": generation["model"],
        }
