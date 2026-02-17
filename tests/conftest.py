"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture
def sample_dataset():
    """Sample dataset for testing."""
    return {
        "identifier": "test-dataset-001",
        "title": "Test Environmental Dataset",
        "abstract": "This dataset contains test environmental measurements.",
        "keywords": ["test", "environment", "data"],
        "topic_categories": ["environment"],
        "lineage": "Generated for testing purposes",
        "bounding_box": {
            "west": -8.0,
            "east": 2.0,
            "north": 60.0,
            "south": 50.0,
        },
        "temporal_extent": {
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
        },
    }


@pytest.fixture
def minimal_dataset():
    """Minimal dataset with only required fields."""
    return {
        "identifier": "test-minimal-001",
        "title": "Minimal Dataset",
        "abstract": "A minimal test abstract for compliance.",
    }
