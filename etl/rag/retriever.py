"""RAG Retrieval Component"""

from typing import Any, Dict, List

from sentence_transformers import SentenceTransformer


class DatasetRetriever:
    def __init__(self, collection, embedding_model: SentenceTransformer):
        self.collection = collection
        self.model = embedding_model

    async def retrieve(
        self,
        query: str,
        top_k: int = 5,
        min_score: float = 0.5,
        include_extracted_text: bool = True,
    ) -> List[Dict[str, Any]]:
        """Retrieve relevant datasets for a query."""
        query_embedding = self.model.encode(query).tolist()

        pipeline = [
            {
                "$vectorSearch": {
                    "index": "vector_index",
                    "path": "embedding",
                    "queryVector": query_embedding,
                    "numCandidates": top_k * 10,
                    "limit": top_k,
                }
            },
            {
                "$addFields": {
                    "relevance_score": {"$meta": "vectorSearchScore"}
                }
            },
            {
                "$project": {
                    "identifier": 1,
                    "title": 1,
                    "abstract": 1,
                    "keywords": 1,
                    "extracted_text": 1,
                    "relevance_score": 1,
                }
            },
        ]

        results = []
        async for doc in self.collection.aggregate(pipeline):
            score = doc.get("relevance_score", 0)
            if score >= min_score:
                results.append(
                    {
                        "id": doc.get("identifier", str(doc.get("_id"))),
                        "title": doc.get("title", ""),
                        "abstract": doc.get("abstract", "")[:1000],
                        "keywords": doc.get("keywords", []),
                        "extracted_text": doc.get("extracted_text", "")[:2000]
                        if include_extracted_text
                        else "",
                        "relevance_score": score,
                    }
                )

        return results
