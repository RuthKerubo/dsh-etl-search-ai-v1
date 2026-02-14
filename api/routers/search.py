"""
Search Router.

GET /search - Hybrid or keyword search with automatic fallback.
"""

import time
from fastapi import APIRouter, Query
from typing import Optional

from api.dependencies import DatasetRepoDep, HybridSearchDep
from api.schemas.responses import SearchResponse, SearchResultItem

router = APIRouter(prefix="/search", tags=["search"])


@router.get("", response_model=SearchResponse)
async def search_datasets(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50, description="Max results"),
    mode: Optional[str] = Query(
        None,
        description="Force search mode: 'hybrid', 'semantic', 'keyword'. Default: auto",
    ),
    repo: DatasetRepoDep = None,
    hybrid_search: HybridSearchDep = None,
) -> SearchResponse:
    """
    Search for datasets.

    Automatically uses the best available search mode:
    - If embeddings configured: hybrid search (semantic + keyword)
    - Otherwise: keyword-only search
    """
    start_time = time.time()

    can_hybrid = hybrid_search is not None

    if mode == "keyword" or (mode is None and not can_hybrid):
        return await _keyword_search(q, limit, repo, start_time)

    elif mode == "semantic" and can_hybrid:
        return await _semantic_search(q, limit, hybrid_search, start_time)

    elif can_hybrid:
        return await _hybrid_search(q, limit, hybrid_search, start_time)

    else:
        return await _keyword_search(q, limit, repo, start_time)


async def _hybrid_search(
    query: str,
    limit: int,
    service,
    start_time: float,
) -> SearchResponse:
    """Perform hybrid search."""
    response = await service.search(query, limit=limit)

    results = [
        SearchResultItem(
            identifier=r.dataset_id,
            title=r.title,
            abstract=r.abstract[:300] if r.abstract else "",
            score=r.hybrid_score,
            keywords=r.keywords[:5],
            from_semantic=r.from_semantic,
            from_keyword=r.from_keyword,
            semantic_rank=r.semantic_rank,
            keyword_rank=r.keyword_rank,
        )
        for r in response.results
    ]

    duration_ms = (time.time() - start_time) * 1000

    return SearchResponse(
        query=query,
        results=results,
        total=len(results),
        mode="hybrid",
        query_type=response.query_type.value,
        semantic_results=response.total_semantic,
        keyword_results=response.total_keyword,
        duration_ms=round(duration_ms, 2),
    )


async def _semantic_search(
    query: str,
    limit: int,
    service,
    start_time: float,
) -> SearchResponse:
    """Perform semantic-only search."""
    results_raw = await service.search_semantic_only(query, limit=limit)

    results = [
        SearchResultItem(
            identifier=r.dataset_id,
            title=r.title,
            abstract=r.abstract[:300] if r.abstract else "",
            score=r.hybrid_score,
            keywords=r.keywords[:5],
            from_semantic=True,
            from_keyword=False,
            semantic_rank=r.semantic_rank,
        )
        for r in results_raw
    ]

    duration_ms = (time.time() - start_time) * 1000

    return SearchResponse(
        query=query,
        results=results,
        total=len(results),
        mode="semantic",
        query_type="normal",
        semantic_results=len(results),
        keyword_results=0,
        duration_ms=round(duration_ms, 2),
    )


async def _keyword_search(
    query: str,
    limit: int,
    repo,
    start_time: float,
) -> SearchResponse:
    """Perform keyword-only search (fallback)."""
    datasets = await repo.search(query, limit=limit)

    results = [
        SearchResultItem(
            identifier=d.identifier,
            title=d.title or "",
            abstract=(d.abstract or "")[:300],
            score=1.0 / (i + 1),
            keywords=d.keywords[:5] if d.keywords else [],
            from_semantic=False,
            from_keyword=True,
            keyword_rank=i + 1,
        )
        for i, d in enumerate(datasets)
    ]

    duration_ms = (time.time() - start_time) * 1000

    return SearchResponse(
        query=query,
        results=results,
        total=len(results),
        mode="keyword",
        query_type="normal",
        semantic_results=0,
        keyword_results=len(results),
        duration_ms=round(duration_ms, 2),
    )
