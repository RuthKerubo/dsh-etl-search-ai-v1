"""Tests for RAG pipeline."""

import pytest
from unittest.mock import MagicMock

from etl.rag.context_builder import build_context
from etl.rag.pipeline import RAGPipeline


# =============================================================================
# Async iterator mock
# =============================================================================


class AsyncIteratorMock:
    """Mock for async MongoDB aggregation cursor."""

    def __init__(self, items):
        self.items = list(items)
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item


# =============================================================================
# Context Builder Tests
# =============================================================================


class TestBuildContext:
    """Tests for context string building."""

    def test_formats_single_doc(self):
        docs = [
            {
                "id": "doc1",
                "title": "Climate Data",
                "abstract": "Temperature measurements",
                "keywords": ["climate", "temperature"],
                "relevance_score": 0.9,
                "extracted_text": "",
            }
        ]
        context = build_context(docs)

        assert "DATASET 1" in context
        assert "Climate Data" in context
        assert "90%" in context
        assert "climate, temperature" in context

    def test_formats_multiple_docs(self):
        docs = [
            {
                "id": f"doc{i}",
                "title": f"Dataset {i}",
                "abstract": f"Abstract {i}",
                "keywords": [],
                "relevance_score": 0.9 - (i * 0.1),
                "extracted_text": "",
            }
            for i in range(3)
        ]
        context = build_context(docs)

        assert "DATASET 1" in context
        assert "DATASET 2" in context
        assert "DATASET 3" in context

    def test_respects_max_chars(self):
        docs = [
            {
                "id": f"doc{i}",
                "title": "A" * 1000,
                "abstract": "B" * 1000,
                "keywords": [],
                "relevance_score": 0.8,
                "extracted_text": "",
            }
            for i in range(10)
        ]
        context = build_context(docs, max_chars=5000)

        # Should have stopped before including all 10 docs
        assert context.count("DATASET") < 10

    def test_includes_extracted_text(self):
        docs = [
            {
                "id": "doc1",
                "title": "Test",
                "abstract": "Abstract",
                "keywords": [],
                "relevance_score": 0.9,
                "extracted_text": "This is extracted content from PDF",
            }
        ]
        context = build_context(docs)

        assert "extracted content" in context

    def test_no_keywords_shows_none(self):
        docs = [
            {
                "id": "doc1",
                "title": "Test",
                "abstract": "Abstract",
                "keywords": [],
                "relevance_score": 0.8,
                "extracted_text": "",
            }
        ]
        context = build_context(docs)

        assert "Keywords: None" in context

    def test_empty_docs_returns_empty(self):
        context = build_context([])
        assert context == ""


# =============================================================================
# RAG Pipeline Tests
# =============================================================================


class TestRAGPipeline:
    """Tests for the full RAG pipeline."""

    @pytest.mark.asyncio
    async def test_no_results_returns_message(self):
        mock_collection = MagicMock()
        mock_collection.aggregate = MagicMock(return_value=AsyncIteratorMock([]))

        mock_model = MagicMock()
        mock_model.encode = MagicMock(return_value=MagicMock(tolist=lambda: [0.1] * 384))

        pipeline = RAGPipeline(mock_collection, mock_model)
        result = await pipeline.query("nonexistent topic", use_llm=False)

        assert "No relevant datasets" in result["answer"]
        assert result["sources"] == []
        assert result["generated"] is False

    @pytest.mark.asyncio
    async def test_results_without_llm(self):
        docs = [
            {
                "identifier": "ds-001",
                "title": "Water Quality Data",
                "abstract": "River water measurements",
                "keywords": ["water", "quality"],
                "extracted_text": "",
                "relevance_score": 0.85,
            }
        ]
        mock_collection = MagicMock()
        mock_collection.aggregate = MagicMock(return_value=AsyncIteratorMock(docs))

        mock_model = MagicMock()
        mock_model.encode = MagicMock(return_value=MagicMock(tolist=lambda: [0.1] * 384))

        pipeline = RAGPipeline(mock_collection, mock_model)
        result = await pipeline.query("water quality", use_llm=False)

        assert result["generated"] is False
        assert result["model"] == "fallback"
        assert len(result["sources"]) == 1
        assert result["sources"][0]["id"] == "ds-001"
        assert result["sources"][0]["title"] == "Water Quality Data"
        assert "relevance_score" in result["sources"][0]

    @pytest.mark.asyncio
    async def test_min_relevance_filters(self):
        docs = [
            {
                "identifier": "ds-high",
                "title": "High Score",
                "abstract": "Relevant",
                "keywords": [],
                "extracted_text": "",
                "relevance_score": 0.9,
            },
            {
                "identifier": "ds-low",
                "title": "Low Score",
                "abstract": "Not very relevant",
                "keywords": [],
                "extracted_text": "",
                "relevance_score": 0.1,
            },
        ]
        mock_collection = MagicMock()
        mock_collection.aggregate = MagicMock(return_value=AsyncIteratorMock(docs))

        mock_model = MagicMock()
        mock_model.encode = MagicMock(return_value=MagicMock(tolist=lambda: [0.1] * 384))

        pipeline = RAGPipeline(mock_collection, mock_model)
        result = await pipeline.query("test", min_relevance=0.5, use_llm=False)

        # Only the high-score doc should survive
        assert len(result["sources"]) == 1
        assert result["sources"][0]["id"] == "ds-high"

    @pytest.mark.asyncio
    async def test_result_structure(self):
        docs = [
            {
                "identifier": "ds-001",
                "title": "Test",
                "abstract": "Abstract",
                "keywords": ["a"],
                "extracted_text": "",
                "relevance_score": 0.8,
            }
        ]
        mock_collection = MagicMock()
        mock_collection.aggregate = MagicMock(return_value=AsyncIteratorMock(docs))

        mock_model = MagicMock()
        mock_model.encode = MagicMock(return_value=MagicMock(tolist=lambda: [0.1] * 384))

        pipeline = RAGPipeline(mock_collection, mock_model)
        result = await pipeline.query("test", use_llm=False)

        assert "question" in result
        assert "answer" in result
        assert "sources" in result
        assert "generated" in result
        assert "model" in result
        assert result["question"] == "test"
