from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from social_listen.collectors.base import BaseCollector
from social_listen.config import AppConfig, EnvSettings
from social_listen.database import Database
from social_listen.engine.relevance import score_post_relevance
from social_listen.models import CollectionResult, RateLimitInfo

logger = logging.getLogger(__name__)


class YouTubeCollector(BaseCollector):
    def __init__(self, config: AppConfig, env: EnvSettings, db: Database):
        self.config = config.collectors.youtube
        self.env = env
        self.db = db
        self._units_used_today = 0
        self._last_reset_date: str | None = None

    @property
    def name(self) -> str:
        return "youtube"

    def _reset_quota_if_needed(self) -> None:
        """Reset daily quota counter at midnight PT."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._last_reset_date != today:
            self._units_used_today = 0
            self._last_reset_date = today

    def _budget_remaining(self) -> int:
        self._reset_quota_if_needed()
        budget = int(self.config.daily_unit_budget * 0.9)  # Use 90% max
        return max(0, budget - self._units_used_today)

    def _build_service(self):
        try:
            from googleapiclient.discovery import build
        except ImportError:
            logger.error("google-api-python-client not installed")
            return None

        if not self.env.youtube_api_key:
            logger.warning("YouTube API key not configured")
            return None

        return build("youtube", "v3", developerKey=self.env.youtube_api_key)

    async def check_health(self) -> bool:
        try:
            service = self._build_service()
            return service is not None
        except Exception as e:
            logger.error(f"YouTube health check failed: {e}")
            return False

    def get_rate_limit_status(self) -> RateLimitInfo:
        return RateLimitInfo(
            remaining=self._budget_remaining(),
            limit=self.config.daily_unit_budget,
        )

    async def collect(self, keywords: list[str]) -> CollectionResult:
        result = CollectionResult(collector="youtube")
        active_keywords = await self.db.get_active_keywords()

        self._reset_quota_if_needed()

        service = self._build_service()
        if service is None:
            result.errors.append("YouTube service not available (check API key)")
            return result

        # Calculate published_after (last 24 hours)
        published_after = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        for query in keywords:
            if self._budget_remaining() < 200:  # Need 100 for search + buffer for channel lookups
                logger.warning("YouTube daily quota nearly exhausted, stopping")
                break

            try:
                await self._search_videos(
                    service, query, published_after, active_keywords, result
                )
            except Exception as e:
                error_msg = str(e)
                logger.error(f"YouTube search error for '{query}': {error_msg}")
                result.errors.append(f"Search error: {error_msg}")

                if "quotaExceeded" in error_msg:
                    logger.warning("YouTube quota exceeded, stopping collection")
                    break

        return result

    async def _search_videos(
        self,
        service,
        query: str,
        published_after: str,
        active_keywords: list[dict],
        result: CollectionResult,
    ) -> None:
        """Search for videos matching a query and process results."""
        import asyncio

        # Run the synchronous API call in a thread pool
        loop = asyncio.get_event_loop()
        search_response = await loop.run_in_executor(
            None,
            lambda: service.search()
            .list(
                q=query,
                type="video",
                order="date",
                publishedAfter=published_after,
                maxResults=self.config.max_results_per_search,
                part="snippet",
            )
            .execute(),
        )
        self._units_used_today += 100  # search costs 100 units

        if not search_response.get("items"):
            return

        # Collect unique channel IDs
        channel_videos: dict[str, list[dict]] = {}
        for item in search_response["items"]:
            channel_id = item["snippet"]["channelId"]
            channel_videos.setdefault(channel_id, []).append(item)

        # Batch fetch channel details (up to 50 per request, 1 unit per request)
        channel_ids = list(channel_videos.keys())
        channel_details = {}

        for i in range(0, len(channel_ids), 50):
            batch = channel_ids[i : i + 50]
            channels_response = await loop.run_in_executor(
                None,
                lambda ids=batch: service.channels()
                .list(
                    id=",".join(ids),
                    part="snippet,statistics",
                )
                .execute(),
            )
            self._units_used_today += 1

            for channel in channels_response.get("items", []):
                channel_details[channel["id"]] = channel

        # Process each channel + its videos
        for channel_id, videos in channel_videos.items():
            channel = channel_details.get(channel_id)
            if not channel:
                continue

            # Check subscriber threshold
            stats = channel.get("statistics", {})
            subscriber_count = int(stats.get("subscriberCount", 0))

            if subscriber_count < self.config.subscriber_threshold:
                continue

            # Upsert channel as platform account
            snippet = channel.get("snippet", {})
            channel_title = snippet.get("title", "Unknown")
            thumbnails = snippet.get("thumbnails", {})
            avatar = thumbnails.get("default", {}).get("url")

            account_id, lead_id, is_new = await self.db.upsert_platform_account(
                platform="youtube",
                platform_user_id=channel_id,
                username=snippet.get("customUrl", channel_title),
                profile_url=f"https://youtube.com/channel/{channel_id}",
                display_name=channel_title,
                bio=snippet.get("description", "")[:500],
                follower_count=subscriber_count,
                avatar_url=avatar,
                raw_data={
                    "view_count": int(stats.get("viewCount", 0)),
                    "video_count": int(stats.get("videoCount", 0)),
                    "country": snippet.get("country"),
                    "published_at": snippet.get("publishedAt"),
                },
            )

            if is_new:
                result.leads_created += 1

            # Upsert each video
            for video_item in videos:
                vid_snippet = video_item.get("snippet", {})
                video_id = video_item.get("id", {}).get("videoId", "")

                content = f"{vid_snippet.get('title', '')} {vid_snippet.get('description', '')}"
                relevance, matched = score_post_relevance(content, active_keywords)

                if relevance == 0:
                    continue

                posted_at = None
                if vid_snippet.get("publishedAt"):
                    try:
                        posted_at = datetime.fromisoformat(
                            vid_snippet["publishedAt"].replace("Z", "+00:00")
                        )
                    except ValueError:
                        pass

                post_id, is_new_post = await self.db.upsert_post(
                    platform_account_id=account_id,
                    platform="youtube",
                    platform_post_id=video_id,
                    content=content[:5000],
                    url=f"https://youtube.com/watch?v={video_id}",
                    post_type="video",
                    engagement={},  # Can't get video stats from search results without extra quota
                    relevance_score=relevance,
                    matched_keywords=matched,
                    posted_at=posted_at,
                )

                if is_new_post:
                    result.posts_found += 1
