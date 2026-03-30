from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import tweepy  # type: ignore

from social_listen.collectors.base import BaseCollector
from social_listen.config import AppConfig, EnvSettings
from social_listen.database import Database
from social_listen.engine.relevance import score_post_relevance
from social_listen.models import CollectionResult, RateLimitInfo

logger = logging.getLogger(__name__)

# Basic tier: 10,000 tweets/month, 60 requests/15min for search
MONTHLY_TWEET_CAP = 10_000
SEARCH_REQUESTS_PER_WINDOW = 60  # per 15 min


class TwitterCollector(BaseCollector):
    def __init__(self, config: AppConfig, env: EnvSettings, db: Database):
        self.config = config.collectors.twitter
        self.env = env
        self.db = db
        self._tweets_read_this_month = 0
        self._requests_this_window = 0
        self._client: tweepy.Client | None = None

    @property
    def name(self) -> str:
        return "twitter"

    def _get_client(self) -> tweepy.Client | None:
        if self._client is not None:
            return self._client

        bearer_token = self.env.twitter_bearer_token
        if not bearer_token:
            logger.warning("Twitter Bearer Token not configured")
            return None

        self._client = tweepy.Client(
            bearer_token=bearer_token,
            wait_on_rate_limit=True,
        )
        return self._client

    async def check_health(self) -> bool:
        try:
            client = self._get_client()
            return client is not None
        except Exception as e:
            logger.error(f"Twitter health check failed: {e}")
            return False

    def get_rate_limit_status(self) -> RateLimitInfo:
        return RateLimitInfo(
            remaining=max(0, MONTHLY_TWEET_CAP - self._tweets_read_this_month),
            limit=MONTHLY_TWEET_CAP,
        )

    async def collect(self, keywords: list[str]) -> CollectionResult:
        result = CollectionResult(collector="twitter")
        active_keywords = await self.db.get_active_keywords()

        client = self._get_client()
        if client is None:
            result.errors.append("Twitter Bearer Token not configured")
            return result

        # Budget: spread 10k tweets across the month
        # With 27 keywords, hourly runs, ~720 runs/month -> ~14 tweets per run per keyword
        # Be conservative: max 10 tweets per keyword per run
        tweets_per_keyword = 10
        budget_remaining = MONTHLY_TWEET_CAP - self._tweets_read_this_month

        if budget_remaining < 100:
            result.errors.append(f"Monthly tweet budget nearly exhausted ({budget_remaining} remaining)")
            return result

        for keyword in keywords:
            if budget_remaining <= 0:
                logger.warning("Twitter monthly budget exhausted")
                break

            try:
                count = await self._search_keyword(
                    client, keyword, active_keywords, result,
                    max_results=min(tweets_per_keyword, budget_remaining),
                )
                budget_remaining -= count
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error searching Twitter for '{keyword}': {error_msg}")
                result.errors.append(f"Search '{keyword}': {error_msg}")

                if "429" in error_msg or "Too Many Requests" in error_msg:
                    logger.warning("Twitter rate limited, stopping")
                    break

            # Small delay between searches
            await asyncio.sleep(1)

        return result

    async def _search_keyword(
        self,
        client: tweepy.Client,
        keyword: str,
        active_keywords: list[dict],
        result: CollectionResult,
        max_results: int = 10,
    ) -> int:
        """Search for a keyword via API v2. Returns number of tweets consumed."""
        loop = asyncio.get_event_loop()

        # API v2 recent search — requires Basic tier
        # max_results must be 10-100
        max_results = max(10, min(max_results, 100))

        # Build query: keyword, exclude retweets, English only
        query = f'"{keyword}" -is:retweet lang:en'
        if len(query) > 512:
            query = f'{keyword} -is:retweet lang:en'

        response = await loop.run_in_executor(
            None,
            lambda: client.search_recent_tweets(
                query=query,
                max_results=max_results,
                tweet_fields=["created_at", "public_metrics", "author_id"],
                user_fields=["username", "name", "description", "public_metrics", "profile_image_url"],
                expansions=["author_id"],
            ),
        )

        if not response.data:
            return 0

        tweets_consumed = len(response.data)
        self._tweets_read_this_month += tweets_consumed

        # Build user lookup from includes
        users_by_id: dict[str, tweepy.User] = {}
        if response.includes and "users" in response.includes:
            for user in response.includes["users"]:
                users_by_id[str(user.id)] = user

        # Process each tweet
        for tweet in response.data:
            await self._process_tweet(tweet, users_by_id, active_keywords, result)

        return tweets_consumed

    async def _process_tweet(
        self,
        tweet: tweepy.Tweet,
        users_by_id: dict[str, tweepy.User],
        active_keywords: list[dict],
        result: CollectionResult,
    ) -> None:
        try:
            author_id = str(tweet.author_id)
            user = users_by_id.get(author_id)
            if user is None:
                return

            # Check follower threshold
            metrics = user.public_metrics or {}
            follower_count = metrics.get("followers_count", 0)
            if follower_count < self.config.follower_threshold:
                return

            # Score relevance
            content = tweet.text or ""
            relevance, matched = score_post_relevance(content, active_keywords)
            if relevance == 0:
                return

            username = user.username or "unknown"
            profile_url = f"https://x.com/{username}"

            # Upsert platform account
            account_id, lead_id, is_new = await self.db.upsert_platform_account(
                platform="twitter",
                platform_user_id=author_id,
                username=username,
                profile_url=profile_url,
                display_name=user.name or username,
                bio=getattr(user, "description", None),
                follower_count=follower_count,
                following_count=metrics.get("following_count", 0),
                avatar_url=getattr(user, "profile_image_url", None),
                raw_data={
                    "verified": getattr(user, "verified", False),
                    "tweet_count": metrics.get("tweet_count", 0),
                    "listed_count": metrics.get("listed_count", 0),
                },
            )

            if is_new:
                result.leads_created += 1

            # Parse tweet timestamp
            posted_at = tweet.created_at
            if posted_at and posted_at.tzinfo is None:
                posted_at = posted_at.replace(tzinfo=timezone.utc)

            # Upsert post
            tweet_metrics = tweet.public_metrics or {}
            tweet_id = str(tweet.id)

            post_id, is_new_post = await self.db.upsert_post(
                platform_account_id=account_id,
                platform="twitter",
                platform_post_id=tweet_id,
                content=content[:5000],
                url=f"https://x.com/{username}/status/{tweet_id}",
                post_type="tweet",
                engagement={
                    "likes": tweet_metrics.get("like_count", 0),
                    "retweets": tweet_metrics.get("retweet_count", 0),
                    "replies": tweet_metrics.get("reply_count", 0),
                    "impressions": tweet_metrics.get("impression_count", 0),
                },
                relevance_score=relevance,
                matched_keywords=matched,
                posted_at=posted_at,
            )

            if is_new_post:
                result.posts_found += 1

        except Exception as e:
            logger.error(f"Error processing tweet {getattr(tweet, 'id', '?')}: {e}")
