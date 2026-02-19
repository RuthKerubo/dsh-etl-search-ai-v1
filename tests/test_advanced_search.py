"""Unit tests for etl/search/advanced.py"""

import pytest

from etl.search.advanced import (
    AdvancedSearchPipeline,
    CrossEncoderReranker,
    FieldWeightedScoring,
    QueryUnderstanding,
)
from etl.search.hybrid import HybridSearchResult


# =============================================================================
# Helpers
# =============================================================================

def make_result(dataset_id, title, abstract="", score=0.5, keywords=None):
    return HybridSearchResult(
        dataset_id=dataset_id,
        title=title,
        abstract=abstract,
        hybrid_score=score,
        keywords=keywords or [],
        from_semantic=True,
    )


# =============================================================================
# QueryUnderstanding
# =============================================================================

class TestQueryUnderstanding:
    def setup_method(self):
        self.qu = QueryUnderstanding()

    def test_temporal_intent_detected(self):
        analysis = self.qu.analyze("river quality 2020")
        assert analysis.has_temporal_intent is True
        assert "temporal" in analysis.intents

    def test_spatial_intent_detected(self):
        analysis = self.qu.analyze("upland species survey scotland")
        assert analysis.has_spatial_intent is True
        assert "spatial" in analysis.intents

    def test_both_intents(self):
        analysis = self.qu.analyze("river quality uk 2021")
        assert analysis.has_temporal_intent is True
        assert analysis.has_spatial_intent is True

    def test_no_intent(self):
        analysis = self.qu.analyze("soil carbon measurement")
        assert analysis.has_temporal_intent is False
        assert analysis.has_spatial_intent is False

    def test_synonyms_expanded(self):
        analysis = self.qu.analyze("river pollution")
        assert "river" in analysis.expanded or any(
            s in analysis.expanded for s in ["stream", "watercourse", "fluvial"]
        )
        assert "pollution" in analysis.expanded or any(
            s in analysis.expanded for s in ["contamination", "contaminant"]
        )

    def test_no_duplicate_synonyms(self):
        # If query already contains a synonym, don't add it again
        analysis = self.qu.analyze("river stream")
        words = analysis.expanded.lower().split()
        assert words.count("river") == 1
        # "stream" is a synonym of "river" — should not be duplicated
        assert words.count("stream") == 1

    def test_original_preserved(self):
        q = "soil carbon flux 2019"
        analysis = self.qu.analyze(q)
        assert analysis.original == q


# =============================================================================
# FieldWeightedScoring
# =============================================================================

class TestFieldWeightedScoring:
    def setup_method(self):
        self.scorer = FieldWeightedScoring(title_weight=2.0, keyword_weight=1.0)

    def test_title_exact_match_boosted(self):
        results = [
            make_result("a", "river water quality", score=0.5),
            make_result("b", "soil carbon flux", score=0.5),
        ]
        rescored = self.scorer.rescore(results, "river water quality")
        assert rescored[0].dataset_id == "a"
        assert rescored[0].hybrid_score > rescored[1].hybrid_score

    def test_title_partial_match_boosted(self):
        results = [
            make_result("a", "river quality index", score=0.5),
            make_result("b", "soil erosion rates", score=0.5),
        ]
        rescored = self.scorer.rescore(results, "river quality")
        assert rescored[0].dataset_id == "a"

    def test_keyword_match_boosted(self):
        results = [
            make_result("a", "Land Survey", score=0.5, keywords=["soil", "land"]),
            make_result("b", "Atmosphere Data", score=0.5, keywords=["climate"]),
        ]
        rescored = self.scorer.rescore(results, "soil")
        assert rescored[0].dataset_id == "a"

    def test_results_sorted_descending(self):
        results = [
            make_result("a", "unrelated", score=0.1),
            make_result("b", "soil carbon", score=0.1),
        ]
        rescored = self.scorer.rescore(results, "soil")
        scores = [r.hybrid_score for r in rescored]
        assert scores == sorted(scores, reverse=True)

    def test_no_match_score_unchanged(self):
        results = [make_result("a", "air quality", score=0.5)]
        original_score = results[0].hybrid_score
        rescored = self.scorer.rescore(results, "zzz_no_match")
        assert rescored[0].hybrid_score == original_score


# =============================================================================
# RRF merge deduplication (via AdvancedSearchPipeline)
# =============================================================================

class TestAdvancedSearchPipeline:
    def setup_method(self):
        self.pipeline = AdvancedSearchPipeline(use_reranker=False)

    def test_pipeline_returns_results(self):
        hybrid = [
            make_result("a", "river quality", score=0.8),
            make_result("b", "soil carbon", score=0.6),
        ]
        result = self.pipeline.search("river", hybrid)
        assert len(result.results) == 2

    def test_query_analysis_populated(self):
        hybrid = [make_result("a", "river quality 2020", score=0.7)]
        result = self.pipeline.search("river quality 2020", hybrid)
        assert result.query_analysis is not None
        assert result.query_analysis.has_temporal_intent is True

    def test_no_duplicates_after_rescoring(self):
        hybrid = [
            make_result("a", "river", score=0.5),
            make_result("b", "river", score=0.5),
            make_result("c", "soil", score=0.4),
        ]
        result = self.pipeline.search("river", hybrid)
        ids = [r.dataset_id for r in result.results]
        assert len(ids) == len(set(ids))

    def test_reranked_false_without_reranker(self):
        hybrid = [make_result("a", "river", score=0.5)]
        result = self.pipeline.search("river", hybrid)
        assert result.reranked is False


# =============================================================================
# CrossEncoderReranker — skipped if model unavailable
# =============================================================================

@pytest.mark.skip(reason="CrossEncoder model requires ~80MB download; skip in CI")
class TestCrossEncoderReranker:
    def test_reranker_changes_order(self):
        reranker = CrossEncoderReranker()
        results = [
            make_result("a", "soil carbon flux in UK peatlands", score=0.4),
            make_result("b", "river water quality monitoring", score=0.5),
        ]
        reranked = reranker.rerank("river quality", results, top_n=2)
        # After reranking on "river quality", "b" should score higher
        assert reranked[0].dataset_id == "b"
