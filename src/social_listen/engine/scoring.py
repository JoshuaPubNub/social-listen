from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from statistics import mean

from social_listen.config import ScoringConfig
from social_listen.database import Database

logger = logging.getLogger(__name__)


class LeadScorer:
    def __init__(self, config: ScoringConfig, db: Database):
        self.weights = config.weights
        self.db = db

    async def rescore_all(self) -> int:
        """Rescore all active (non-merged) leads. Returns count of leads rescored."""
        cursor = await self.db.conn.execute(
            "SELECT id FROM leads WHERE merged_into_id IS NULL"
        )
        leads = await cursor.fetchall()

        count = 0
        for row in leads:
            try:
                score = await self._calculate_score(row["id"])
                await self.db.update_lead_score(row["id"], score)
                count += 1
            except Exception as e:
                logger.error(f"Error scoring lead {row['id']}: {e}")

        return count

    async def _calculate_score(self, lead_id: int) -> float:
        """Calculate composite lead score (0-100)."""
        accounts = await self.db.get_platform_accounts_for_lead(lead_id)
        posts = await self.db.get_posts_for_lead(lead_id)

        if not accounts:
            return 0.0

        audience = self._score_audience(accounts)
        relevance = self._score_relevance(posts)
        engagement = self._score_engagement(posts)
        recency = self._score_recency(posts)

        composite = (
            audience * self.weights.audience
            + relevance * self.weights.relevance
            + engagement * self.weights.engagement
            + recency * self.weights.recency
        )

        return round(composite, 1)

    def _score_audience(self, accounts: list[dict]) -> float:
        """Score based on follower count (0-100). Log-scaled."""
        if not accounts:
            return 0.0

        max_followers = max(a.get("follower_count", 0) for a in accounts)
        if max_followers <= 0:
            return 0.0

        # log10(1000)=3, log10(100000)=5, log10(1000000)=6
        # Map range [3, 6] to [0, 100]
        log_val = math.log10(max(max_followers, 1))
        score = (log_val - 3) / 3 * 100
        return max(0, min(score, 100))

    def _score_relevance(self, posts: list[dict]) -> float:
        """Score based on average post relevance (0-100)."""
        if not posts:
            return 0.0

        scores = [p.get("relevance_score", 0) for p in posts]
        return mean(scores) * 100

    def _score_engagement(self, posts: list[dict]) -> float:
        """Score based on engagement metrics (0-100)."""
        if not posts:
            return 0.0

        engagement_scores = []
        for post in posts:
            eng = post.get("engagement")
            if isinstance(eng, str):
                try:
                    eng = json.loads(eng)
                except (json.JSONDecodeError, TypeError):
                    eng = {}

            if not eng or not isinstance(eng, dict):
                continue

            # Normalize engagement across platforms
            score = 0.0
            if "score" in eng:  # Reddit
                score = min(eng["score"] / 100, 1.0) * 100
            elif "likes" in eng:  # Twitter
                likes = eng.get("likes", 0) or 0
                retweets = eng.get("retweets", 0) or 0
                score = min((likes + retweets * 2) / 50, 1.0) * 100
            elif "views" in eng:  # YouTube
                views = eng.get("views", 0) or 0
                score = min(views / 10000, 1.0) * 100

            engagement_scores.append(score)

        return mean(engagement_scores) if engagement_scores else 0.0

    def _score_recency(self, posts: list[dict]) -> float:
        """Score based on how recently the person posted (0-100)."""
        if not posts:
            return 0.0

        now = datetime.now(timezone.utc)
        most_recent = None

        for post in posts:
            posted_at = post.get("posted_at")
            if posted_at:
                if isinstance(posted_at, str):
                    try:
                        dt = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
                    except ValueError:
                        continue
                elif isinstance(posted_at, datetime):
                    dt = posted_at
                else:
                    continue

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

                if most_recent is None or dt > most_recent:
                    most_recent = dt

        if most_recent is None:
            return 0.0

        days_ago = (now - most_recent).total_seconds() / 86400
        # Lose 5 points per day, minimum 0
        return max(100 - (days_ago * 5), 0)
