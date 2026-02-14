"""
Domain models for the ETL pipeline.

Pydantic models for validation and serialization.
With MongoDB, these are stored directly as documents
(no ORM layer or converters needed).
"""

# Domain models (Pydantic)
from .dataset import (
    # Enums
    AccessType,
    RelationshipType,
    ResponsiblePartyRole,
    TopicCategory,
    # Component models
    BoundingBox,
    DistributionInfo,
    RelatedDocument,
    ResponsibleParty,
    SupportingDocument,
    TemporalExtent,
    # Main model
    DatasetMetadata,
    # Factory functions
    create_minimal_dataset,
)

from .user import (
    User,
    UserCreate,
    UserUpdate,
    UserPreferences,
    SearchHistoryEntry,
)

__all__ = [
    # Domain Enums
    "AccessType",
    "RelationshipType",
    "ResponsiblePartyRole",
    "TopicCategory",
    # Domain Component models
    "BoundingBox",
    "DistributionInfo",
    "RelatedDocument",
    "ResponsibleParty",
    "SupportingDocument",
    "TemporalExtent",
    # Domain Main model
    "DatasetMetadata",
    # Domain Factory functions
    "create_minimal_dataset",
    # Users
    "User",
    "UserCreate",
    "UserUpdate",
    "UserPreferences",
    "SearchHistoryEntry",
]
