"""Tests for ISO 19115 compliance checker."""

import pytest

from etl.validation.iso_compliance import (
    ALL_FIELDS,
    RECOMMENDED_FIELDS,
    REQUIRED_FIELDS,
    check_compliance,
)


# =============================================================================
# Required Fields
# =============================================================================


class TestRequiredFields:
    """Tests for required field validation."""

    def test_all_required_present(self, sample_dataset):
        result = check_compliance(sample_dataset)
        assert result["compliant"] is True
        assert len(result["missing_required"]) == 0

    def test_missing_abstract(self):
        dataset = {"identifier": "test-002", "title": "Test Dataset"}
        result = check_compliance(dataset)
        assert result["compliant"] is False
        assert "abstract" in result["missing_required"]

    def test_missing_title(self):
        dataset = {"identifier": "test", "abstract": "Some abstract text"}
        result = check_compliance(dataset)
        assert result["compliant"] is False
        assert "title" in result["missing_required"]

    def test_missing_identifier(self):
        dataset = {"title": "Test", "abstract": "Some abstract text"}
        result = check_compliance(dataset)
        assert result["compliant"] is False
        assert "identifier" in result["missing_required"]

    def test_empty_string_counts_as_missing(self):
        dataset = {"identifier": "test", "title": "", "abstract": "Valid abstract"}
        result = check_compliance(dataset)
        assert result["compliant"] is False
        assert "title" in result["missing_required"]

    def test_whitespace_only_counts_as_missing(self):
        dataset = {"identifier": "test", "title": "   ", "abstract": "Valid abstract"}
        result = check_compliance(dataset)
        assert result["compliant"] is False
        assert "title" in result["missing_required"]

    def test_none_counts_as_missing(self):
        dataset = {"identifier": "test", "title": None, "abstract": "Valid abstract"}
        result = check_compliance(dataset)
        assert result["compliant"] is False
        assert "title" in result["missing_required"]


# =============================================================================
# Recommended Fields
# =============================================================================


class TestRecommendedFields:
    """Tests for recommended field validation."""

    def test_all_recommended_present(self, sample_dataset):
        result = check_compliance(sample_dataset)
        assert len(result["missing_recommended"]) == 0

    def test_missing_keywords(self, minimal_dataset):
        result = check_compliance(minimal_dataset)
        assert "keywords" in result["missing_recommended"]

    def test_missing_lineage(self, minimal_dataset):
        result = check_compliance(minimal_dataset)
        assert "lineage" in result["missing_recommended"]

    def test_missing_bounding_box(self, minimal_dataset):
        result = check_compliance(minimal_dataset)
        assert "bounding_box" in result["missing_recommended"]

    def test_missing_temporal_extent(self, minimal_dataset):
        result = check_compliance(minimal_dataset)
        assert "temporal_extent" in result["missing_recommended"]

    def test_empty_list_counts_as_missing(self):
        dataset = {
            "identifier": "test",
            "title": "Test Title",
            "abstract": "Test abstract text",
            "keywords": [],
        }
        result = check_compliance(dataset)
        assert "keywords" in result["missing_recommended"]

    def test_empty_dict_counts_as_missing(self):
        dataset = {
            "identifier": "test",
            "title": "Test Title",
            "abstract": "Test abstract text",
            "bounding_box": {},
        }
        result = check_compliance(dataset)
        assert "bounding_box" in result["missing_recommended"]


# =============================================================================
# Score Calculation
# =============================================================================


class TestScoreCalculation:
    """Tests for compliance score."""

    def test_full_dataset_scores_100(self, sample_dataset):
        result = check_compliance(sample_dataset)
        assert result["score"] == 100

    def test_minimal_dataset_scores_lower(self, minimal_dataset):
        result = check_compliance(minimal_dataset)
        # 3 required fields present out of 8 total
        expected = round((3 / len(ALL_FIELDS)) * 100)
        assert result["score"] == expected

    def test_empty_dataset_scores_zero(self):
        result = check_compliance({})
        assert result["score"] == 0

    def test_more_fields_means_higher_score(self, minimal_dataset):
        minimal_result = check_compliance(minimal_dataset)

        fuller = dict(minimal_dataset)
        fuller["keywords"] = ["test"]
        fuller["lineage"] = "Some lineage"
        fuller_result = check_compliance(fuller)

        assert fuller_result["score"] > minimal_result["score"]

    def test_score_is_percentage(self, sample_dataset):
        result = check_compliance(sample_dataset)
        assert 0 <= result["score"] <= 100


# =============================================================================
# Warnings
# =============================================================================


class TestWarnings:
    """Tests for compliance warnings."""

    def test_short_title_warning(self):
        dataset = {"identifier": "test", "title": "Hi", "abstract": "Valid abstract text here"}
        result = check_compliance(dataset)
        assert any("Title is very short" in w for w in result["warnings"])

    def test_short_abstract_warning(self):
        dataset = {"identifier": "test", "title": "Valid Title", "abstract": "Short"}
        result = check_compliance(dataset)
        assert any("Abstract is very short" in w for w in result["warnings"])

    def test_no_short_warning_for_valid_fields(self, sample_dataset):
        result = check_compliance(sample_dataset)
        assert not any("very short" in w for w in result["warnings"])

    def test_missing_required_generates_warning(self, minimal_dataset):
        # Remove abstract to trigger required warning
        dataset = {"identifier": "test", "title": "Test"}
        result = check_compliance(dataset)
        assert any("Missing required" in w for w in result["warnings"])

    def test_missing_recommended_generates_warning(self, minimal_dataset):
        result = check_compliance(minimal_dataset)
        assert any("Missing recommended" in w for w in result["warnings"])


# =============================================================================
# Return Structure
# =============================================================================


class TestReturnStructure:
    """Tests for the compliance result dict structure."""

    def test_result_has_all_keys(self):
        result = check_compliance({})
        assert "compliant" in result
        assert "score" in result
        assert "missing_required" in result
        assert "missing_recommended" in result
        assert "warnings" in result

    def test_compliant_is_bool(self, sample_dataset):
        result = check_compliance(sample_dataset)
        assert isinstance(result["compliant"], bool)

    def test_score_is_int(self, sample_dataset):
        result = check_compliance(sample_dataset)
        assert isinstance(result["score"], int)

    def test_missing_fields_are_lists(self, sample_dataset):
        result = check_compliance(sample_dataset)
        assert isinstance(result["missing_required"], list)
        assert isinstance(result["missing_recommended"], list)

    def test_warnings_is_list(self, sample_dataset):
        result = check_compliance(sample_dataset)
        assert isinstance(result["warnings"], list)
