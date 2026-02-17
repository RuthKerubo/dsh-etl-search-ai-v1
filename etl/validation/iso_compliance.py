"""
ISO 19115 metadata compliance checker.

Checks dataset metadata against ISO 19115 required and recommended fields
and returns a compliance report with score, missing fields, and warnings.
"""


# ISO 19115 required fields (must have non-empty value)
REQUIRED_FIELDS = [
    "title",
    "abstract",
    "identifier",
]

# ISO 19115 recommended fields (should have non-empty value)
RECOMMENDED_FIELDS = [
    "keywords",
    "topic_categories",
    "lineage",
    "bounding_box",
    "temporal_extent",
]

# All fields used for scoring
ALL_FIELDS = REQUIRED_FIELDS + RECOMMENDED_FIELDS


def _is_present(value) -> bool:
    """Check if a field value is meaningfully present."""
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    if isinstance(value, list) and len(value) == 0:
        return False
    if isinstance(value, dict) and not value:
        return False
    return True


def check_compliance(dataset: dict) -> dict:
    """
    Check a dataset dict against ISO 19115 metadata requirements.

    Args:
        dataset: Dictionary of dataset fields (from pending doc or dataset doc).

    Returns:
        dict with keys:
            compliant: bool — True if all required fields are present
            score: int — percentage of all fields present (0-100)
            missing_required: list[str] — required fields that are missing
            missing_recommended: list[str] — recommended fields that are missing
            warnings: list[str] — human-readable warnings
    """
    missing_required = [f for f in REQUIRED_FIELDS if not _is_present(dataset.get(f))]
    missing_recommended = [f for f in RECOMMENDED_FIELDS if not _is_present(dataset.get(f))]

    present_count = len(ALL_FIELDS) - len(missing_required) - len(missing_recommended)
    score = round((present_count / len(ALL_FIELDS)) * 100) if ALL_FIELDS else 0

    warnings = []
    if missing_required:
        warnings.append(f"Missing required fields: {', '.join(missing_required)}")
    if missing_recommended:
        warnings.append(f"Missing recommended fields: {', '.join(missing_recommended)}")

    title = dataset.get("title", "")
    if isinstance(title, str) and len(title.strip()) < 5:
        warnings.append("Title is very short (less than 5 characters)")

    abstract = dataset.get("abstract", "")
    if isinstance(abstract, str) and 0 < len(abstract.strip()) < 20:
        warnings.append("Abstract is very short (less than 20 characters)")

    return {
        "compliant": len(missing_required) == 0,
        "score": score,
        "missing_required": missing_required,
        "missing_recommended": missing_recommended,
        "warnings": warnings,
    }
