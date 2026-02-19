"""
Search Router.

GET /search - Hybrid or keyword search with automatic fallback.
"""

import time
from fastapi import APIRouter, Query
from typing import Optional

from api.auth.dependencies import OptionalUser
from api.dependencies import DatasetRepoDep, HybridSearchDep
from api.schemas.responses import SearchResponse, SearchResultItem
from etl.guardrails import DataGuardrails
from etl.search.advanced import AdvancedSearchPipeline

router = APIRouter(prefix="/search", tags=["search"])


@router.get("", response_model=SearchResponse)
async def search_datasets(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50, description="Max results"),
    mode: Optional[str] = Query(
        None,
        description="Force search mode: 'hybrid', 'semantic', 'keyword'. Default: auto",
    ),
    advanced: bool = Query(False, description="Enable advanced pipeline (query expansion + reranking)"),
    current_user: OptionalUser = None,
    repo: DatasetRepoDep = None,
    hybrid_search: HybridSearchDep = None,
) -> SearchResponse:
    """
    Search for datasets.

    Automatically uses the best available search mode:
    - If embeddings configured: hybrid search (semantic + keyword)
    - Otherwise: keyword-only search

    Access control: anonymous users only see public datasets.
    """
    start_time = time.time()
    user_role = current_user.get("role") if current_user else None

    can_hybrid = hybrid_search is not None

    if mode == "keyword" or (mode is None and not can_hybrid):
        response = await _keyword_search(q, limit, repo, start_time)
    elif mode == "semantic" and can_hybrid:
        response = await _semantic_search(q, limit, hybrid_search, start_time)
    elif can_hybrid:
        response = await _hybrid_search(q, limit, hybrid_search, start_time, advanced)
    else:
        response = await _keyword_search(q, limit, repo, start_time)

    # Apply access-level guardrails
    filtered = DataGuardrails.filter_datasets_by_access(
        [r.model_dump() for r in response.results],
        user_role,
    )
    response.results = [SearchResultItem(**d) for d in filtered]
    response.total = len(response.results)

    return response


async def _hybrid_search(
    query: str,
    limit: int,
    service,
    start_time: float,
    advanced: bool = False,
) -> SearchResponse:
    """Perform hybrid search, optionally with advanced pipeline."""
    hybrid_response = await service.search(query, limit=limit)

    hybrid_results = hybrid_response.results
    query_analysis_dict: Optional[dict] = None
    expanded_query: Optional[str] = None

    if advanced:
        pipeline = AdvancedSearchPipeline(use_reranker=True)
        advanced_result = pipeline.search(query, hybrid_results)
        hybrid_results = advanced_result.results
        query_analysis_dict = {
            "intents": advanced_result.query_analysis.intents,
            "has_temporal_intent": advanced_result.query_analysis.has_temporal_intent,
            "has_spatial_intent": advanced_result.query_analysis.has_spatial_intent,
            "synonyms_added": advanced_result.query_analysis.synonyms_added,
            "reranked": advanced_result.reranked,
        }
        expanded_query = advanced_result.query_analysis.expanded

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
            access_level=getattr(r, "access_level", "public"),
        )
        for r in hybrid_results
    ]

    duration_ms = (time.time() - start_time) * 1000

    return SearchResponse(
        query=query,
        results=results,
        total=len(results),
        mode="hybrid",
        query_type=hybrid_response.query_type.value,
        semantic_results=hybrid_response.total_semantic,
        keyword_results=hybrid_response.total_keyword,
        duration_ms=round(duration_ms, 2),
        query_analysis=query_analysis_dict,
        expanded_query=expanded_query,
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
            access_level=getattr(r, "access_level", "public"),
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
            access_level=getattr(d, "access_level", "public"),
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
