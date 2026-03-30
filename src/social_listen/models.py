from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class Lead(BaseModel):
    id: int
    display_name: str | None = None
    notes: str | None = None
    status: str = "new"
    lead_score: float = 0.0
    first_seen_at: datetime | None = None
    updated_at: datetime | None = None
    merged_into_id: int | None = None
    # Joined data
    platform_accounts: list[PlatformAccount] = []
    posts: list[Post] = []


class PlatformAccount(BaseModel):
    id: int = 0
    lead_id: int = 0
    platform: str
    platform_user_id: str
    username: str | None = None
    profile_url: str | None = None
    display_name: str | None = None
    bio: str | None = None
    follower_count: int = 0
    following_count: int = 0
    avatar_url: str | None = None
    raw_data: dict[str, Any] | None = None
    last_checked_at: datetime | None = None
    created_at: datetime | None = None


class Post(BaseModel):
    id: int = 0
    platform_account_id: int = 0
    platform: str
    platform_post_id: str
    content: str | None = None
    url: str | None = None
    post_type: str | None = None
    engagement: dict[str, Any] | None = None
    relevance_score: float = 0.0
    matched_keywords: list[str] = []
    posted_at: datetime | None = None
    discovered_at: datetime | None = None


class CollectionResult(BaseModel):
    collector: str
    posts_found: int = 0
    leads_created: int = 0
    errors: list[str] = []


class RateLimitInfo(BaseModel):
    remaining: int = 0
    limit: int = 0
    resets_at: datetime | None = None


class Keyword(BaseModel):
    id: int = 0
    term: str
    category: str | None = None
    is_active: bool = True
    added_at: datetime | None = None


class CollectionRun(BaseModel):
    id: int = 0
    collector: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    status: str = "running"
    posts_found: int = 0
    leads_created: int = 0
    error_message: str | None = None
    metadata: dict[str, Any] | None = None


# Allow forward references
Lead.model_rebuild()
