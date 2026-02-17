"""Main RAG Pipeline"""

from typing import Any, Dict

from .context_builder import build_context
from .generator import generate_answer
from .retriever import DatasetRetriever


class RAGPipeline:
    def __init__(self, collection, embedding_model):
        self.retriever = DatasetRetriever(collection, embedding_model)

    async def query(
        self,
        question: str,
        top_k: int = 5,
        min_relevance: float = 0.3,
        use_llm: bool = True,
    ) -> Dict[str, Any]:
        """Full RAG pipeline."""

        # 1. Retrieve
        retrieved = await self.retriever.retrieve(
            query=question,
            top_k=top_k,
            min_score=min_relevance,
        )

        if not retrieved:
            return {
                "question": question,
                "answer": "No relevant datasets found. Try different keywords.",
                "sources": [],
                "generated": False,
                "model": None,
            }

        # 2. Build context
        context = build_context(retrieved)

        # 3. Generate
        if use_llm:
            generation = await generate_answer(question, context)
        else:
            generation = {
                "answer": f"Found {len(retrieved)} relevant datasets:\n\n{context}",
                "model": "fallback",
                "generated": False,
            }

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
