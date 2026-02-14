"""
Base data structures for repositories.
"""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class PagedResult(Generic[T]):
    """Result of a paginated query."""
    items: list[T]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        if self.page_size <= 0:
            return 0
        return (self.total + self.page_size - 1) // self.page_size

    @property
    def has_next(self) -> bool:
        return self.page < self.total_pages

    @property
    def has_previous(self) -> bool:
        return self.page > 1


@dataclass
class BulkOperationResult:
    """Result of a bulk operation (save_many, delete_many)."""
    succeeded: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)  # (id, error)

    def add_success(self, identifier: str) -> None:
        self.succeeded.append(identifier)

    def add_failure(self, identifier: str, error: str) -> None:
        self.failed.append((identifier, error))

    @property
    def success_count(self) -> int:
        return len(self.succeeded)

    @property
    def failure_count(self) -> int:
        return len(self.failed)

    @property
    def total_count(self) -> int:
        return self.success_count + self.failure_count

    @property
    def all_succeeded(self) -> bool:
        return self.failure_count == 0
