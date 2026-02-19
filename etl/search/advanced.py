"""
Advanced Search Pipeline.

Components:
  - QueryUnderstanding  : intent detection + synonym expansion
  - FieldWeightedScoring: boost results by title/keyword match
  - CrossEncoderReranker: optional neural reranking (lazy-loaded)
  - AdvancedSearchPipeline: orchestrates the above

No new pip dependencies — CrossEncoder is part of sentence-transformers.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from etl.search.hybrid import HybridSearchResult


# =============================================================================
# Synonym / expansion dictionaries
# =============================================================================

_SYNONYMS: dict[str, list[str]] = {
    "soil": ["land", "sediment", "substrate", "earth"],
    "water": ["aquatic", "hydrological", "hydrology", "aquifer"],
    "river": ["stream", "watercourse", "fluvial", "tributary"],
    "rain": ["rainfall", "precipitation", "downfall"],
    "climate": ["weather", "meteorological", "atmospheric"],
    "species": ["taxa", "biodiversity", "organism", "fauna", "flora"],
    "carbon": ["co2", "greenhouse gas", "sequestration", "organic matter"],
    "pollution": ["contamination", "contaminant", "pollutant"],
    "flood": ["inundation", "floodplain", "flooding"],
    "habitat": ["ecosystem", "biotope", "environment"],
    "temperature": ["thermal", "heat", "warming"],
    "nitrogen": ["nitrate", "nitrite", "nutrient"],
    "phosphorus": ["phosphate", "nutrient"],
    "woodland": ["forest", "tree", "canopy", "woodland"],
    "peat": ["peatland", "bog", "mire", "fen"],
}

# Temporal intent patterns
_TEMPORAL_PATTERNS = re.compile(
    r"\b(\d{4}|\d{4}s|recent|historic|long.term|decad|annual|monthly|seasonal)\b",
    re.IGNORECASE,
)

# Spatial intent patterns
_SPATIAL_PATTERNS = re.compile(
    r"\b(uk|england|scotland|wales|ireland|north|south|east|west|coastal|"
    r"upland|lowland|catchment|watershed|national park)\b",
    re.IGNORECASE,
)


# =============================================================================
# Query Understanding
# =============================================================================

@dataclass
class QueryAnalysis:
    """Results of query intent analysis."""
    original: str
    expanded: str
    has_temporal_intent: bool = False
    has_spatial_intent: bool = False
    intents: list[str] = field(default_factory=list)
    synonyms_added: list[str] = field(default_factory=list)


class QueryUnderstanding:
    """
    Detects query intents and expands the query with domain synonyms.

    Usage::

        qu = QueryUnderstanding()
        analysis = qu.analyze("river water quality uk 2020")
        print(analysis.expanded)
        # "river stream watercourse water aquatic quality uk 2020"
    """

    def analyze(self, query: str) -> QueryAnalysis:
        """Analyse and expand a query."""
        intents: list[str] = []

        has_temporal = bool(_TEMPORAL_PATTERNS.search(query))
        has_spatial = bool(_SPATIAL_PATTERNS.search(query))

        if has_temporal:
            intents.append("temporal")
        if has_spatial:
            intents.append("spatial")

        expanded, synonyms_added = self._expand(query)

        return QueryAnalysis(
            original=query,
            expanded=expanded,
            has_temporal_intent=has_temporal,
            has_spatial_intent=has_spatial,
            intents=intents,
            synonyms_added=synonyms_added,
        )

    def _expand(self, query: str) -> tuple[str, list[str]]:
        """Return expanded query and list of added synonym tokens."""
        words = query.lower().split()
        extra_tokens: list[str] = []
        already_in_query = set(words)

        for word in words:
            syns = _SYNONYMS.get(word, [])
            for syn in syns:
                if syn not in already_in_query:
                    extra_tokens.append(syn)
                    already_in_query.add(syn)

        if extra_tokens:
            expanded = query + " " + " ".join(extra_tokens)
        else:
            expanded = query

        return expanded, extra_tokens


# =============================================================================
# Field-Weighted Scoring
# =============================================================================

class FieldWeightedScoring:
    """
    Boost hybrid search results by field-level match quality.

    Weights:
      - Title exact match  : +title_weight (default 2.0)
      - Title partial match: +title_weight * 0.5
      - Keyword exact match: +keyword_weight (default 1.0)
    """

    def __init__(
        self,
        title_weight: float = 2.0,
        keyword_weight: float = 1.0,
    ):
        self.title_weight = title_weight
        self.keyword_weight = keyword_weight

    def rescore(
        self,
        results: list[HybridSearchResult],
        query: str,
    ) -> list[HybridSearchResult]:
        """Apply field-weighted boost and re-sort in-place."""
        q = query.lower()

        for r in results:
            title = (r.title or "").lower()

            if q == title:
                r.hybrid_score += self.title_weight
            elif q in title:
                r.hybrid_score += self.title_weight * 0.5

            if any(q == kw.lower() for kw in r.keywords):
                r.hybrid_score += self.keyword_weight

        results.sort(key=lambda x: x.hybrid_score, reverse=True)
        return results


# =============================================================================
# Cross-Encoder Reranker (lazy-loaded)
# =============================================================================

class CrossEncoderReranker:
    """
    Neural cross-encoder reranker using sentence-transformers.

    The model is lazy-loaded on first call to avoid startup overhead (~80 MB).
    Adds ~100-500 ms latency when reranking top-N results.

    Default model: cross-encoder/ms-marco-MiniLM-L-6-v2
    """

    DEFAULT_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"

    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name
        self._model = None  # Lazy-loaded

    def _load_model(self):
        if self._model is None:
            try:
                from sentence_transformers import CrossEncoder
                self._model = CrossEncoder(self.model_name)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to load CrossEncoder model '{self.model_name}': {exc}"
                ) from exc

    def rerank(
        self,
        query: str,
        results: list[HybridSearchResult],
        top_n: int = 10,
    ) -> list[HybridSearchResult]:
        """
        Rerank the top-N results using the cross-encoder.

        Results beyond top_n are appended unchanged after the reranked set.

        Args:
            query: The search query.
            results: Current ranked results (from hybrid/RRF).
            top_n: How many top results to rerank.

        Returns:
            Reranked + remaining results.
        """
        self._load_model()

        to_rerank = results[:top_n]
        remainder = results[top_n:]

        if not to_rerank:
            return results

        pairs = [(query, f"{r.title}. {(r.abstract or '')[:200]}") for r in to_rerank]
        scores = self._model.predict(pairs)  # type: ignore[union-attr]

        for result, score in zip(to_rerank, scores):
            result.hybrid_score = float(score)

        to_rerank.sort(key=lambda x: x.hybrid_score, reverse=True)
        return to_rerank + remainder


# =============================================================================
# Advanced Search Pipeline
# =============================================================================

@dataclass
class AdvancedSearchResult:
    """Output of AdvancedSearchPipeline.search()."""
    results: list[HybridSearchResult]
    query_analysis: QueryAnalysis
    reranked: bool = False


class AdvancedSearchPipeline:
    """
    Orchestrates query understanding, field-weighted scoring, and optional
    cross-encoder reranking over an existing hybrid search response.

    Usage::

        pipeline = AdvancedSearchPipeline(use_reranker=True)
        advanced = pipeline.search(
            query="river water quality",
            hybrid_results=hybrid_response.results,
        )
        print(advanced.query_analysis.expanded)
        for r in advanced.results:
            print(r.hybrid_score, r.title)
    """

    def __init__(
        self,
        use_reranker: bool = True,
        reranker_model: str = CrossEncoderReranker.DEFAULT_MODEL,
        title_weight: float = 2.0,
        keyword_weight: float = 1.0,
        rerank_top_n: int = 10,
    ):
        self.query_understanding = QueryUnderstanding()
        self.field_scorer = FieldWeightedScoring(title_weight, keyword_weight)
        self.use_reranker = use_reranker
        self.rerank_top_n = rerank_top_n
        self._reranker: Optional[CrossEncoderReranker] = None

        if use_reranker:
            self._reranker = CrossEncoderReranker(reranker_model)

    def search(
        self,
        query: str,
        hybrid_results: list[HybridSearchResult],
    ) -> AdvancedSearchResult:
        """
        Apply advanced scoring on top of existing hybrid results.

        Args:
            query: Original search query.
            hybrid_results: Results from HybridSearchService (RRF-merged).

        Returns:
            AdvancedSearchResult with re-scored / reranked results.
        """
        # 1. Understand the query
        analysis = self.query_understanding.analyze(query)

        # 2. Field-weighted boost
        results = self.field_scorer.rescore(list(hybrid_results), analysis.expanded)

        # 3. Optional cross-encoder reranking
        reranked = False
        if self.use_reranker and self._reranker is not None:
            try:
                results = self._reranker.rerank(query, results, top_n=self.rerank_top_n)
                reranked = True
            except RuntimeError:
                # Model unavailable — skip reranking silently
                pass

        return AdvancedSearchResult(
            results=results,
            query_analysis=analysis,
            reranked=reranked,
        )
