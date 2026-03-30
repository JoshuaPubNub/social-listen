from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import asyncpraw  # type: ignore

from social_listen.collectors.base import BaseCollector
from social_listen.config import AppConfig, EnvSettings
from social_listen.database import Database
from social_listen.engine.relevance import score_post_relevance
from social_listen.models import CollectionResult, RateLimitInfo

logger = logging.getLogger(__name__)


class RedditCollector(BaseCollector):
    def __init__(self, config: AppConfig, env: EnvSettings, db: Database):
        self.config = config.collectors.reddit
        self.env = env
        self.db = db
        self._rate_limit_remaining = 100
        self._rate_limit_total = 100

    @property
    def name(self) -> str:
        return "reddit"

    async def _create_client(self) -> asyncpraw.Reddit:
        return asyncpraw.Reddit(
            client_id=self.env.reddit_client_id,
            client_secret=self.env.reddit_client_secret,
            user_agent=self.env.reddit_user_agent,
        )

    async def check_health(self) -> bool:
        try:
            reddit = await self._create_client()
            async with reddit:
                sub = await reddit.subreddit("test")
                _ = sub.display_name
            return True
        except Exception as e:
            logger.error(f"Reddit health check failed: {e}")
            return False

    def get_rate_limit_status(self) -> RateLimitInfo:
        return RateLimitInfo(
            remaining=self._rate_limit_remaining,
            limit=self._rate_limit_total,
        )

    async def collect(self, keywords: list[str]) -> CollectionResult:
        result = CollectionResult(collector="reddit")
        active_keywords = await self.db.get_active_keywords()

        if not self.env.reddit_client_id or not self.env.reddit_client_secret:
            result.errors.append("Reddit credentials not configured")
            return result

        reddit = await self._create_client()
        async with reddit:
            # Strategy 1: Search within target subreddits
            for sub_name in self.config.subreddits:
                try:
                    subreddit = await reddit.subreddit(sub_name)
                    for keyword in keywords:
                        try:
                            async for submission in subreddit.search(
                                keyword,
                                sort="new",
                                time_filter="day",
                                limit=self.config.posts_per_keyword,
                            ):
                                await self._process_submission(
                                    submission, active_keywords, result
                                )
                        except Exception as e:
                            logger.warning(f"Error searching r/{sub_name} for '{keyword}': {e}")
                            result.errors.append(f"r/{sub_name} search error: {e}")
                        # Small delay between keyword searches
                        await asyncio.sleep(0.5)
                except Exception as e:
                    logger.warning(f"Error accessing r/{sub_name}: {e}")
                    result.errors.append(f"r/{sub_name} access error: {e}")

            # Strategy 2: Global search for core keywords
            try:
                all_sub = await reddit.subreddit("all")
                core_keywords = [
                    kw for kw in keywords
                    if any(
                        k["term"] == kw and k.get("category") == "core"
                        for k in active_keywords
                    )
                ]
                for keyword in core_keywords[:5]:  # Limit global searches
                    try:
                        async for submission in all_sub.search(
                            keyword,
                            sort="new",
                            time_filter="day",
                            limit=10,
                        ):
                            await self._process_submission(
                                submission, active_keywords, result
                            )
                    except Exception as e:
                        logger.warning(f"Error in r/all search for '{keyword}': {e}")
                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Error searching r/all: {e}")

        return result

    async def _process_submission(
        self,
        submission,
        active_keywords: list[dict],
        result: CollectionResult,
    ) -> None:
        """Process a single Reddit submission."""
        try:
            # Get author info
            author = submission.author
            if author is None:
                return

            # Fetch author details
            try:
                await author.load()
            except Exception:
                return  # Deleted/suspended account

            # Calculate combined karma
            combined_karma = getattr(author, "link_karma", 0) + getattr(author, "comment_karma", 0)

            # Filter by karma threshold
            if combined_karma < self.config.karma_threshold:
                return

            # Score relevance
            content = f"{submission.title} {getattr(submission, 'selftext', '')}"
            relevance, matched = score_post_relevance(content, active_keywords)

            if relevance == 0:
                return  # No keyword matches (shouldn't happen often from search)

            # Build profile URL
            username = author.name
            profile_url = f"https://reddit.com/user/{username}"

            # Upsert platform account + lead
            account_id, lead_id, is_new = await self.db.upsert_platform_account(
                platform="reddit",
                platform_user_id=str(author.id) if hasattr(author, "id") else username,
                username=username,
                profile_url=profile_url,
                display_name=getattr(author, "subreddit", {}).get("title", username) if hasattr(author, "subreddit") else username,
                bio=getattr(author, "subreddit", {}).get("public_description", None) if hasattr(author, "subreddit") else None,
                follower_count=combined_karma,
                avatar_url=getattr(author, "icon_img", None),
                raw_data={
                    "link_karma": getattr(author, "link_karma", 0),
                    "comment_karma": getattr(author, "comment_karma", 0),
                    "created_utc": getattr(author, "created_utc", None),
                    "is_gold": getattr(author, "is_gold", False),
                },
            )

            if is_new:
                result.leads_created += 1

            # Upsert post
            posted_at = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
            post_id, is_new_post = await self.db.upsert_post(
                platform_account_id=account_id,
                platform="reddit",
                platform_post_id=str(submission.id),
                content=content[:5000],  # Truncate very long posts
                url=f"https://reddit.com{submission.permalink}",
                post_type="reddit_post",
                engagement={
                    "score": submission.score,
                    "upvote_ratio": getattr(submission, "upvote_ratio", None),
                    "num_comments": submission.num_comments,
                },
                relevance_score=relevance,
                matched_keywords=matched,
                posted_at=posted_at,
            )

            if is_new_post:
                result.posts_found += 1

        except Exception as e:
            logger.error(f"Error processing submission {getattr(submission, 'id', '?')}: {e}")
