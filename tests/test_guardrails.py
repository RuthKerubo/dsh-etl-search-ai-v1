"""Unit tests for etl/guardrails/filters.py"""

import pytest

from etl.guardrails.filters import DataGuardrails, RAGGuardrails


# =============================================================================
# DataGuardrails
# =============================================================================

class TestAllowedAccessLevels:
    def test_anonymous(self):
        allowed = DataGuardrails.allowed_access_levels(None)
        assert allowed == {"public"}

    def test_researcher(self):
        allowed = DataGuardrails.allowed_access_levels("researcher")
        assert "public" in allowed
        assert "restricted" in allowed
        assert "admin_only" not in allowed

    def test_admin(self):
        allowed = DataGuardrails.allowed_access_levels("admin")
        assert allowed == {"public", "restricted", "admin_only"}

    def test_unknown_role_treated_as_anonymous(self):
        allowed = DataGuardrails.allowed_access_levels("mystery_role")
        assert allowed == {"public"}


class TestFilterDatasetsByAccess:
    DATASETS = [
        {"identifier": "a", "title": "Public A",    "access_level": "public"},
        {"identifier": "b", "title": "Restricted B", "access_level": "restricted"},
        {"identifier": "c", "title": "Admin C",      "access_level": "admin_only"},
        {"identifier": "d", "title": "No Level",     },  # missing key → defaults to public
    ]

    def test_anonymous_sees_only_public(self):
        visible = DataGuardrails.filter_datasets_by_access(self.DATASETS, None)
        ids = {d["identifier"] for d in visible}
        assert ids == {"a", "d"}

    def test_researcher_sees_public_and_restricted(self):
        visible = DataGuardrails.filter_datasets_by_access(self.DATASETS, "researcher")
        ids = {d["identifier"] for d in visible}
        assert ids == {"a", "b", "d"}

    def test_admin_sees_all(self):
        visible = DataGuardrails.filter_datasets_by_access(self.DATASETS, "admin")
        assert len(visible) == len(self.DATASETS)

    def test_empty_list(self):
        assert DataGuardrails.filter_datasets_by_access([], "admin") == []

    def test_preserves_order(self):
        visible = DataGuardrails.filter_datasets_by_access(self.DATASETS, "admin")
        assert [d["identifier"] for d in visible] == ["a", "b", "c", "d"]


class TestCheckQuerySensitivity:
    def test_detects_embargoed(self):
        assert DataGuardrails.check_query_sensitivity("embargoed species data") is True

    def test_detects_protected_species(self):
        assert DataGuardrails.check_query_sensitivity("protected species habitat") is True

    def test_detects_restricted(self):
        assert DataGuardrails.check_query_sensitivity("restricted area survey") is True

    def test_normal_query_not_sensitive(self):
        assert DataGuardrails.check_query_sensitivity("soil carbon flux") is False

    def test_case_insensitive(self):
        assert DataGuardrails.check_query_sensitivity("CLASSIFIED document") is True


# =============================================================================
# RAGGuardrails
# =============================================================================

class TestRedactPii:
    def test_email_redacted(self):
        text = "Contact admin@example.com for access."
        result = RAGGuardrails.redact_pii(text)
        assert "admin@example.com" not in result
        assert "[EMAIL REDACTED]" in result

    def test_uk_phone_redacted(self):
        text = "Call us on 07911 123456 for details."
        result = RAGGuardrails.redact_pii(text)
        assert "07911 123456" not in result
        assert "[PHONE REDACTED]" in result

    def test_postcode_redacted(self):
        text = "The site is located at SW1A 1AA in London."
        result = RAGGuardrails.redact_pii(text)
        assert "SW1A 1AA" not in result
        assert "[POSTCODE REDACTED]" in result

    def test_no_pii_unchanged(self):
        text = "Soil carbon data from the UK uplands."
        assert RAGGuardrails.redact_pii(text) == text


class TestValidateResponse:
    def test_pii_flagged_as_redacted(self):
        result = RAGGuardrails.validate_response("Email user@test.org for info.")
        assert result["redacted"] is True
        assert "user@test.org" not in result["response"]

    def test_clean_response_not_flagged(self):
        result = RAGGuardrails.validate_response("The dataset covers UK river systems.")
        assert result["redacted"] is False
        assert result["response"] == "The dataset covers UK river systems."

    def test_filter_context_by_access_delegates(self):
        docs = [
            {"id": "x", "access_level": "public"},
            {"id": "y", "access_level": "admin_only"},
        ]
        filtered = RAGGuardrails.filter_context_by_access(docs, user_role=None)
        assert len(filtered) == 1
        assert filtered[0]["id"] == "x"
