from __future__ import annotations

from abc import ABC, abstractmethod

from social_listen.models import CollectionResult, RateLimitInfo


class BaseCollector(ABC):
    """Abstract base for all platform collectors."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Collector name (e.g., 'twitter', 'reddit', 'youtube')."""
        ...

    @abstractmethod
    async def collect(self, keywords: list[str]) -> CollectionResult:
        """Run a collection cycle for the given keywords.

        Search the platform for posts matching keywords, filter authors
        by follower threshold, and upsert leads + posts into the database.
        """
        ...

    @abstractmethod
    async def check_health(self) -> bool:
        """Check if the collector can reach its platform API."""
        ...

    @abstractmethod
    def get_rate_limit_status(self) -> RateLimitInfo:
        """Return current rate limit status."""
        ...
