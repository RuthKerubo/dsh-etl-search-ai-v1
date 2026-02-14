"""
Sentence Transformer embedding service implementation.
Free, local embeddings - no API key or rate limits needed.
"""

from typing import List, Optional

from sentence_transformers import SentenceTransformer

from .base import EmbeddingService


class SentenceTransformerService(EmbeddingService):
    """Embedding service using sentence-transformers (local, free)."""

    def __init__(self, model_name: Optional[str] = None):
        model_name = model_name or "sentence-transformers/all-MiniLM-L6-v2"
        self._model = SentenceTransformer(model_name)
        self._model_name = model_name
        self._dimensions = self._model.get_sentence_embedding_dimension()

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def dimensions(self) -> int:
        return self._dimensions

    async def embed_query(self, text: str) -> List[float]:
        """Embed a single query text."""
        embedding = self._model.encode(text)
        return embedding.tolist()

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed a batch of texts."""
        embeddings = self._model.encode(
            texts,
            batch_size=32,
            show_progress_bar=True,
        )
        return [emb.tolist() for emb in embeddings]
