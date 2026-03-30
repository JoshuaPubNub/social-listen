from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from social_listen.collectors.reddit import RedditCollector
from social_listen.collectors.twitter import TwitterCollector
from social_listen.collectors.youtube import YouTubeCollector
from social_listen.config import AppConfig, EnvSettings
from social_listen.database import Database
from social_listen.engine.keywords import KeywordManager
from social_listen.engine.scoring import LeadScorer

logger = logging.getLogger(__name__)


class CollectionScheduler:
    def __init__(self, config: AppConfig, env: EnvSettings, db: Database):
        self.config = config
        self.env = env
        self.db = db
        self.keyword_manager = KeywordManager(db)
        self.scorer = LeadScorer(config.scoring, db)

        self.collectors = {}
        if config.collectors.twitter.enabled:
            self.collectors["twitter"] = TwitterCollector(config, env, db)
        if config.collectors.reddit.enabled:
            self.collectors["reddit"] = RedditCollector(config, env, db)
        if config.collectors.youtube.enabled:
            self.collectors["youtube"] = YouTubeCollector(config, env, db)

        self.scheduler = AsyncIOScheduler()
        # Track consecutive failures for backoff
        self._failure_counts: dict[str, int] = {name: 0 for name in self.collectors}

    def start(self) -> None:
        """Start the collection scheduler."""
        # Reddit: every 30 minutes
        if "reddit" in self.collectors:
            self.scheduler.add_job(
                self._run_collector,
                "interval",
                minutes=self.config.collectors.reddit.interval_minutes,
                args=["reddit"],
                id="reddit_collector",
                max_instances=1,
                misfire_grace_time=300,
            )

        # Twitter: every 60 minutes
        if "twitter" in self.collectors:
            self.scheduler.add_job(
                self._run_collector,
                "interval",
                minutes=self.config.collectors.twitter.interval_minutes,
                args=["twitter"],
                id="twitter_collector",
                max_instances=1,
                misfire_grace_time=300,
            )

        # YouTube: every 6 hours
        if "youtube" in self.collectors:
            self.scheduler.add_job(
                self._run_collector,
                "interval",
                hours=self.config.collectors.youtube.interval_hours,
                args=["youtube"],
                id="youtube_collector",
                max_instances=1,
                misfire_grace_time=600,
            )

        # Lead rescoring: every hour
        self.scheduler.add_job(
            self._rescore_leads,
            "interval",
            hours=1,
            id="lead_rescoring",
            max_instances=1,
        )

        # Stagger initial runs so they don't all fire at startup
        now = datetime.now(timezone.utc)
        if "reddit" in self.collectors:
            self.scheduler.add_job(
                self._run_collector,
                "date",
                run_date=now + timedelta(seconds=10),
                args=["reddit"],
                id="reddit_initial",
            )
        if "twitter" in self.collectors:
            self.scheduler.add_job(
                self._run_collector,
                "date",
                run_date=now + timedelta(seconds=30),
                args=["twitter"],
                id="twitter_initial",
            )
        if "youtube" in self.collectors:
            self.scheduler.add_job(
                self._run_collector,
                "date",
                run_date=now + timedelta(seconds=60),
                args=["youtube"],
                id="youtube_initial",
            )

        # Initial scoring run after collectors have had time to finish
        self.scheduler.add_job(
            self._rescore_leads,
            "date",
            run_date=now + timedelta(seconds=90),
            id="scoring_initial",
        )

        self.scheduler.start()
        logger.info(
            f"Scheduler started with collectors: {list(self.collectors.keys())}"
        )

    def stop(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")

    async def _run_collector(self, collector_name: str) -> None:
        """Run a single collector with error handling and observability."""
        collector = self.collectors.get(collector_name)
        if not collector:
            return

        run_id = await self.db.start_collection_run(
            collector_name,
            metadata={"failure_count": self._failure_counts.get(collector_name, 0)},
        )

        try:
            # Get keywords optimized for this platform
            keywords = await self.keyword_manager.get_search_queries_for_platform(
                collector_name
            )

            if not keywords:
                logger.warning(f"No active keywords for {collector_name}")
                await self.db.finish_collection_run(run_id, "completed")
                return

            logger.info(
                f"Starting {collector_name} collection with {len(keywords)} queries"
            )
            result = await collector.collect(keywords)

            # Record success
            error_msg = "; ".join(result.errors) if result.errors else None
            status = "completed" if not result.errors else "completed"  # Non-fatal errors are still "completed"
            await self.db.finish_collection_run(
                run_id,
                status=status,
                posts_found=result.posts_found,
                leads_created=result.leads_created,
                error_message=error_msg,
            )

            self._failure_counts[collector_name] = 0

            # Score leads immediately after collection
            if result.posts_found > 0 or result.leads_created > 0:
                try:
                    scored = await self.scorer.rescore_all()
                    logger.info(f"Rescored {scored} leads after {collector_name} collection")
                except Exception as e:
                    logger.error(f"Post-collection scoring failed: {e}")

            logger.info(
                f"{collector_name} collection done: "
                f"{result.posts_found} posts, {result.leads_created} new leads"
                f"{f', {len(result.errors)} errors' if result.errors else ''}"
            )

        except Exception as e:
            self._failure_counts[collector_name] = (
                self._failure_counts.get(collector_name, 0) + 1
            )
            error_msg = f"{type(e).__name__}: {e}"
            logger.error(f"{collector_name} collection failed: {error_msg}")

            await self.db.finish_collection_run(
                run_id,
                status="failed",
                error_message=error_msg,
            )

            # Exponential backoff: reschedule with increased delay
            failures = self._failure_counts[collector_name]
            if failures <= 5:
                backoff_minutes = min(2 ** failures * 5, 240)  # Max 4 hours
                logger.warning(
                    f"{collector_name} backoff: {backoff_minutes}min "
                    f"(failure #{failures})"
                )

    async def _rescore_leads(self) -> None:
        """Rescore all leads."""
        try:
            count = await self.scorer.rescore_all()
            logger.info(f"Rescored {count} leads")
        except Exception as e:
            logger.error(f"Lead rescoring failed: {e}")
