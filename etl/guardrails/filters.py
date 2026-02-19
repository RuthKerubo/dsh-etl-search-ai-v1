"""
Guardrail filters for access control and response validation.

DataGuardrails: Controls which datasets are visible based on user role.
RAGGuardrails: Filters RAG context and redacts PII from responses.
"""

from __future__ import annotations

import re
from typing import Optional


# =============================================================================
# Role → access level mapping
# =============================================================================

# Roles that can see each access level (cumulative — higher roles include lower)
_ROLE_ALLOWED_LEVELS: dict[str | None, set[str]] = {
    None: {"public"},               # Anonymous / no token
    "researcher": {"public", "restricted"},
    "admin": {"public", "restricted", "admin_only"},
}

# Sensitive query patterns that warrant extra caution
_SENSITIVE_PATTERNS = re.compile(
    r"\b(embargoed|sensitive|protected.species|restricted.area|classified|confidential)\b",
    re.IGNORECASE,
)

# PII patterns for redaction
_PII_PATTERNS = [
    # Email addresses
    (re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"), "[EMAIL REDACTED]"),
    # UK phone numbers (various formats)
    (re.compile(r"\b(?:(?:\+44|0044|0)\s?(?:\d\s?){9,10})\b"), "[PHONE REDACTED]"),
    # UK postcodes
    (re.compile(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b", re.IGNORECASE), "[POSTCODE REDACTED]"),
]


class DataGuardrails:
    """
    Controls dataset visibility based on user role and access level.

    Access levels (from DatasetMetadata.AccessLevel):
      - public     : visible to everyone
      - restricted : requires researcher or admin role
      - admin_only : requires admin role
    """

    @staticmethod
    def allowed_access_levels(user_role: Optional[str]) -> set[str]:
        """Return the set of access levels visible to the given role."""
        # Normalise unknown roles to anonymous
        if user_role not in _ROLE_ALLOWED_LEVELS:
            return _ROLE_ALLOWED_LEVELS[None]
        return _ROLE_ALLOWED_LEVELS[user_role]

    @classmethod
    def filter_datasets_by_access(
        cls,
        datasets: list[dict],
        user_role: Optional[str] = None,
    ) -> list[dict]:
        """
        Filter a list of dataset dicts to only those the user may see.

        Args:
            datasets: List of dataset dicts (must have an ``access_level`` key;
                      missing key defaults to ``"public"``).
            user_role: The authenticated user's role, or None for anonymous.

        Returns:
            Filtered list preserving original order.
        """
        allowed = cls.allowed_access_levels(user_role)
        return [
            d for d in datasets
            if d.get("access_level", "public") in allowed
        ]

    @staticmethod
    def check_query_sensitivity(query: str) -> bool:
        """Return True if the query touches sensitive topics."""
        return bool(_SENSITIVE_PATTERNS.search(query))


class RAGGuardrails:
    """
    Guardrails for the RAG pipeline.

    - filter_context_by_access: drops restricted docs from RAG context
    - validate_response: redacts PII from the generated answer
    """

    @staticmethod
    def filter_context_by_access(
        docs: list[dict],
        user_role: Optional[str] = None,
    ) -> list[dict]:
        """
        Remove documents the user is not authorised to see from RAG context.

        Delegates to DataGuardrails for the actual access check.
        """
        return DataGuardrails.filter_datasets_by_access(docs, user_role)

    @staticmethod
    def redact_pii(text: str) -> str:
        """Replace recognised PII patterns with redaction placeholders."""
        for pattern, replacement in _PII_PATTERNS:
            text = pattern.sub(replacement, text)
        return text

    @classmethod
    def validate_response(
        cls,
        response: str,
        user_role: Optional[str] = None,
    ) -> dict:
        """
        Validate and sanitise a RAG-generated response.

        Always redacts PII. Returns a dict with ``response`` and
        ``redacted`` (True if any PII was removed).

        Args:
            response: The raw LLM-generated answer.
            user_role: The user's role (reserved for future role-based rules).

        Returns:
            ``{"response": str, "redacted": bool}``
        """
        cleaned = cls.redact_pii(response)
        return {
            "response": cleaned,
            "redacted": cleaned != response,
        }
