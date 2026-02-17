"""Tests for search router functionality."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from api.schemas.responses import SearchResponse, SearchResultItem
from etl.models.dataset import DatasetMetadata


# =============================================================================
# Search Result Schema Tests
# =============================================================================


class TestSearchResultItem:
    """Tests for SearchResultItem schema."""

    def test_create_result_item(self):
        item = SearchResultItem(
            identifier="test-123",
            title="Test Dataset",
            abstract="Test abstract",
            score=0.85,
            keywords=["test"],
            from_semantic=True,
            from_keyword=False,
            semantic_rank=1,
        )
        assert item.identifier == "test-123"
        assert item.score == 0.85
        assert item.from_semantic is True

    def test_defaults(self):
        item = SearchResultItem(
            identifier="test",
            title="Test",
            abstract="Test",
            score=0.5,
        )
        assert item.keywords == []
        assert item.from_semantic is False
        assert item.from_keyword is False
        assert item.semantic_rank is None
        assert item.keyword_rank is None


class TestSearchResponse:
    """Tests for SearchResponse schema."""

    def test_create_response(self):
        response = SearchResponse(
            query="test query",
            results=[],
            total=0,
            mode="keyword",
            query_type="normal",
            duration_ms=12.5,
        )
        assert response.query == "test query"
        assert response.total == 0
        assert response.mode == "keyword"

    def test_response_with_results(self):
        items = [
            SearchResultItem(
                identifier=f"id-{i}",
                title=f"Dataset {i}",
                abstract=f"Abstract {i}",
                score=1.0 / (i + 1),
            )
            for i in range(3)
        ]
        response = SearchResponse(
            query="water",
            results=items,
            total=3,
            mode="hybrid",
            query_type="normal",
            semantic_results=2,
            keyword_results=3,
            duration_ms=45.2,
        )
        assert len(response.results) == 3
        assert response.semantic_results == 2
        assert response.keyword_results == 3


# =============================================================================
# Keyword Search Logic Tests
# =============================================================================


class TestKeywordSearchLogic:
    """Tests for keyword search fallback logic."""

    @pytest.mark.asyncio
    async def test_keyword_search_returns_results(self):
        """Test the keyword search path with mocked repo."""
        from api.routers.search import _keyword_search
        import time

        mock_repo = AsyncMock()
        mock_repo.search = AsyncMock(return_value=[
            DatasetMetadata(
                identifier="ds-001",
                title="UK River Flow Data",
                abstract="River discharge measurements across UK",
                keywords=["river", "flow", "hydrology"],
            ),
            DatasetMetadata(
                identifier="ds-002",
                title="Water Quality Report",
                abstract="Water quality analysis results",
                keywords=["water", "quality"],
            ),
        ])

        result = await _keyword_search("river water", 10, mock_repo, time.time())

        assert isinstance(result, SearchResponse)
        assert result.mode == "keyword"
        assert len(result.results) == 2
        assert result.results[0].identifier == "ds-001"
        assert result.results[0].from_keyword is True
        assert result.results[0].from_semantic is False
        assert result.results[0].keyword_rank == 1

    @pytest.mark.asyncio
    async def test_keyword_search_empty_results(self):
        """Test keyword search with no matches."""
        from api.routers.search import _keyword_search
        import time

        mock_repo = AsyncMock()
        mock_repo.search = AsyncMock(return_value=[])

        result = await _keyword_search("nonexistent", 10, mock_repo, time.time())

        assert result.total == 0
        assert result.results == []

    @pytest.mark.asyncio
    async def test_keyword_search_score_decreases(self):
        """Test that keyword search assigns decreasing scores by rank."""
        from api.routers.search import _keyword_search
        import time

        datasets = [
            DatasetMetadata(identifier=f"ds-{i}", title=f"Dataset {i}", keywords=[])
            for i in range(5)
        ]
        mock_repo = AsyncMock()
        mock_repo.search = AsyncMock(return_value=datasets)

        result = await _keyword_search("test", 10, mock_repo, time.time())

        scores = [r.score for r in result.results]
        # Scores should be strictly decreasing
        for i in range(len(scores) - 1):
            assert scores[i] > scores[i + 1]

    @pytest.mark.asyncio
    async def test_keyword_search_truncates_abstract(self):
        """Test that long abstracts are truncated."""
        from api.routers.search import _keyword_search
        import time

        mock_repo = AsyncMock()
        mock_repo.search = AsyncMock(return_value=[
            DatasetMetadata(
                identifier="ds-long",
                title="Long Abstract Dataset",
                abstract="A" * 500,
                keywords=[],
            ),
        ])

        result = await _keyword_search("test", 10, mock_repo, time.time())

        assert len(result.results[0].abstract) <= 300
