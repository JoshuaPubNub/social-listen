from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute("PRAGMA journal_mode=WAL")
        await self._connection.execute("PRAGMA busy_timeout=5000")
        await self._connection.execute("PRAGMA foreign_keys=ON")

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
            self._connection = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._connection

    async def initialize(self) -> None:
        """Run schema.sql to create tables."""
        schema = SCHEMA_PATH.read_text()
        # Use executescript via the underlying connection (bypasses row_factory issue)
        await self.conn.executescript(schema)
        await self.conn.commit()
        logger.info("Database schema initialized")

    # ── Lead operations ──────────────────────────────────────────────

    async def create_lead(self, display_name: str | None = None) -> int:
        cursor = await self.conn.execute(
            "INSERT INTO leads (display_name) VALUES (?)",
            (display_name,),
        )
        await self.conn.commit()
        return cursor.lastrowid  # type: ignore

    async def get_lead(self, lead_id: int) -> dict | None:
        cursor = await self.conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def update_lead_score(self, lead_id: int, score: float) -> None:
        await self.conn.execute(
            "UPDATE leads SET lead_score = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (score, lead_id),
        )
        await self.conn.commit()

    async def update_lead_status(self, lead_id: int, status: str) -> None:
        await self.conn.execute(
            "UPDATE leads SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, lead_id),
        )
        await self.conn.commit()

    async def update_lead_notes(self, lead_id: int, notes: str) -> None:
        await self.conn.execute(
            "UPDATE leads SET notes = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (notes, lead_id),
        )
        await self.conn.commit()

    async def merge_leads(self, primary_id: int, secondary_id: int) -> None:
        """Merge secondary lead into primary: move accounts, mark as merged."""
        await self.conn.execute(
            "UPDATE platform_accounts SET lead_id = ? WHERE lead_id = ?",
            (primary_id, secondary_id),
        )
        await self.conn.execute(
            "UPDATE leads SET merged_into_id = ?, status = 'ignored', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (primary_id, secondary_id),
        )
        await self.conn.commit()

    async def get_leads_paginated(
        self,
        page: int = 1,
        page_size: int = 50,
        platform: str | None = None,
        status: str | None = None,
        min_score: float | None = None,
        sort: str = "score",
    ) -> tuple[list[dict], int]:
        """Return paginated leads with optional filters. Returns (leads, total_count)."""
        conditions = ["l.merged_into_id IS NULL"]
        params: list = []

        if platform:
            conditions.append("EXISTS (SELECT 1 FROM platform_accounts pa WHERE pa.lead_id = l.id AND pa.platform = ?)")
            params.append(platform)
        if status:
            conditions.append("l.status = ?")
            params.append(status)
        if min_score is not None:
            conditions.append("l.lead_score >= ?")
            params.append(min_score)

        where = " AND ".join(conditions)

        sort_map = {
            "score": "l.lead_score DESC",
            "newest": "l.first_seen_at DESC",
            "followers": "l.lead_score DESC",  # approximate
            "name": "l.display_name ASC",
        }
        order_by = sort_map.get(sort, "l.lead_score DESC")

        # Count
        count_cursor = await self.conn.execute(
            f"SELECT COUNT(*) FROM leads l WHERE {where}", params
        )
        total = (await count_cursor.fetchone())[0]

        # Fetch page
        offset = (page - 1) * page_size
        cursor = await self.conn.execute(
            f"SELECT l.* FROM leads l WHERE {where} ORDER BY {order_by} LIMIT ? OFFSET ?",
            params + [page_size, offset],
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows], total

    # ── Platform account operations ──────────────────────────────────

    async def upsert_platform_account(
        self,
        platform: str,
        platform_user_id: str,
        username: str | None = None,
        profile_url: str | None = None,
        display_name: str | None = None,
        bio: str | None = None,
        follower_count: int = 0,
        following_count: int = 0,
        avatar_url: str | None = None,
        raw_data: dict | None = None,
    ) -> tuple[int, int, bool]:
        """Upsert a platform account. Returns (account_id, lead_id, is_new_lead)."""
        # Check if account already exists
        cursor = await self.conn.execute(
            "SELECT id, lead_id FROM platform_accounts WHERE platform = ? AND platform_user_id = ?",
            (platform, platform_user_id),
        )
        existing = await cursor.fetchone()

        raw_json = json.dumps(raw_data) if raw_data else None

        if existing:
            # Update existing account
            await self.conn.execute(
                """UPDATE platform_accounts
                   SET username = COALESCE(?, username),
                       profile_url = COALESCE(?, profile_url),
                       display_name = COALESCE(?, display_name),
                       bio = COALESCE(?, bio),
                       follower_count = ?,
                       following_count = ?,
                       avatar_url = COALESCE(?, avatar_url),
                       raw_data = COALESCE(?, raw_data),
                       last_checked_at = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (username, profile_url, display_name, bio, follower_count,
                 following_count, avatar_url, raw_json, existing["id"]),
            )
            await self.conn.commit()
            # Also update the lead's display_name if we have a better one
            if display_name:
                await self.conn.execute(
                    "UPDATE leads SET display_name = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND display_name IS NULL",
                    (display_name, existing["lead_id"]),
                )
                await self.conn.commit()
            return existing["id"], existing["lead_id"], False

        # Create new lead + account
        lead_id = await self.create_lead(display_name)
        cursor = await self.conn.execute(
            """INSERT INTO platform_accounts
               (lead_id, platform, platform_user_id, username, profile_url,
                display_name, bio, follower_count, following_count, avatar_url,
                raw_data, last_checked_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (lead_id, platform, platform_user_id, username, profile_url,
             display_name, bio, follower_count, following_count, avatar_url, raw_json),
        )
        await self.conn.commit()
        return cursor.lastrowid, lead_id, True  # type: ignore

    async def get_platform_accounts_for_lead(self, lead_id: int) -> list[dict]:
        cursor = await self.conn.execute(
            "SELECT * FROM platform_accounts WHERE lead_id = ?", (lead_id,)
        )
        return [dict(r) for r in await cursor.fetchall()]

    # ── Post operations ──────────────────────────────────────────────

    async def upsert_post(
        self,
        platform_account_id: int,
        platform: str,
        platform_post_id: str,
        content: str | None = None,
        url: str | None = None,
        post_type: str | None = None,
        engagement: dict | None = None,
        relevance_score: float = 0.0,
        matched_keywords: list[str] | None = None,
        posted_at: datetime | None = None,
    ) -> tuple[int, bool]:
        """Upsert a post. Returns (post_id, is_new)."""
        cursor = await self.conn.execute(
            "SELECT id FROM posts WHERE platform = ? AND platform_post_id = ?",
            (platform, platform_post_id),
        )
        existing = await cursor.fetchone()

        engagement_json = json.dumps(engagement) if engagement else None
        keywords_json = json.dumps(matched_keywords) if matched_keywords else None

        if existing:
            await self.conn.execute(
                """UPDATE posts
                   SET content = COALESCE(?, content),
                       engagement = COALESCE(?, engagement),
                       relevance_score = ?,
                       matched_keywords = COALESCE(?, matched_keywords)
                   WHERE id = ?""",
                (content, engagement_json, relevance_score, keywords_json, existing["id"]),
            )
            await self.conn.commit()
            return existing["id"], False

        cursor = await self.conn.execute(
            """INSERT INTO posts
               (platform_account_id, platform, platform_post_id, content, url,
                post_type, engagement, relevance_score, matched_keywords, posted_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (platform_account_id, platform, platform_post_id, content, url,
             post_type, engagement_json, relevance_score, keywords_json, posted_at),
        )
        await self.conn.commit()
        return cursor.lastrowid, True  # type: ignore

    async def get_posts_for_lead(self, lead_id: int) -> list[dict]:
        cursor = await self.conn.execute(
            """SELECT p.* FROM posts p
               JOIN platform_accounts pa ON p.platform_account_id = pa.id
               WHERE pa.lead_id = ?
               ORDER BY p.posted_at DESC""",
            (lead_id,),
        )
        return [dict(r) for r in await cursor.fetchall()]

    # ── Collection run operations ────────────────────────────────────

    async def start_collection_run(self, collector: str, metadata: dict | None = None) -> int:
        cursor = await self.conn.execute(
            "INSERT INTO collection_runs (collector, started_at, metadata) VALUES (?, CURRENT_TIMESTAMP, ?)",
            (collector, json.dumps(metadata) if metadata else None),
        )
        await self.conn.commit()
        return cursor.lastrowid  # type: ignore

    async def finish_collection_run(
        self, run_id: int, status: str, posts_found: int = 0,
        leads_created: int = 0, error_message: str | None = None,
    ) -> None:
        await self.conn.execute(
            """UPDATE collection_runs
               SET finished_at = CURRENT_TIMESTAMP, status = ?,
                   posts_found = ?, leads_created = ?, error_message = ?
               WHERE id = ?""",
            (status, posts_found, leads_created, error_message, run_id),
        )
        await self.conn.commit()

    async def get_recent_runs(self, limit: int = 50) -> list[dict]:
        cursor = await self.conn.execute(
            "SELECT * FROM collection_runs ORDER BY started_at DESC LIMIT ?", (limit,)
        )
        return [dict(r) for r in await cursor.fetchall()]

    async def get_last_run(self, collector: str) -> dict | None:
        cursor = await self.conn.execute(
            "SELECT * FROM collection_runs WHERE collector = ? ORDER BY started_at DESC LIMIT 1",
            (collector,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    # ── Keyword operations ───────────────────────────────────────────

    async def get_active_keywords(self) -> list[dict]:
        cursor = await self.conn.execute(
            "SELECT * FROM keywords WHERE is_active = 1 ORDER BY category, term"
        )
        return [dict(r) for r in await cursor.fetchall()]

    async def get_all_keywords(self) -> list[dict]:
        cursor = await self.conn.execute("SELECT * FROM keywords ORDER BY category, term")
        return [dict(r) for r in await cursor.fetchall()]

    async def add_keyword(self, term: str, category: str | None = None) -> int | None:
        try:
            cursor = await self.conn.execute(
                "INSERT INTO keywords (term, category) VALUES (?, ?)", (term, category)
            )
            await self.conn.commit()
            return cursor.lastrowid
        except Exception:
            return None

    async def toggle_keyword(self, keyword_id: int) -> None:
        await self.conn.execute(
            "UPDATE keywords SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE id = ?",
            (keyword_id,),
        )
        await self.conn.commit()

    async def seed_keywords(self, keywords: dict[str, list[str]]) -> None:
        """Seed keywords from a category->terms dict. Skips existing terms."""
        for category, terms in keywords.items():
            for term in terms:
                await self.conn.execute(
                    "INSERT OR IGNORE INTO keywords (term, category) VALUES (?, ?)",
                    (term, category),
                )
        await self.conn.commit()

    # ── Stats ────────────────────────────────────────────────────────

    async def get_stats(self) -> dict:
        stats = {}

        cursor = await self.conn.execute(
            "SELECT COUNT(*) FROM leads WHERE merged_into_id IS NULL"
        )
        stats["total_leads"] = (await cursor.fetchone())[0]

        cursor = await self.conn.execute(
            "SELECT COUNT(*) FROM leads WHERE merged_into_id IS NULL AND first_seen_at >= date('now')"
        )
        stats["leads_today"] = (await cursor.fetchone())[0]

        cursor = await self.conn.execute(
            "SELECT COUNT(*) FROM leads WHERE merged_into_id IS NULL AND first_seen_at >= date('now', '-7 days')"
        )
        stats["leads_this_week"] = (await cursor.fetchone())[0]

        cursor = await self.conn.execute("SELECT COUNT(*) FROM posts")
        stats["total_posts"] = (await cursor.fetchone())[0]

        cursor = await self.conn.execute(
            """SELECT pa.platform, COUNT(DISTINCT pa.lead_id) as count
               FROM platform_accounts pa
               JOIN leads l ON pa.lead_id = l.id
               WHERE l.merged_into_id IS NULL
               GROUP BY pa.platform"""
        )
        stats["leads_by_platform"] = {row["platform"]: row["count"] for row in await cursor.fetchall()}

        cursor = await self.conn.execute(
            """SELECT l.status, COUNT(*) as count
               FROM leads l
               WHERE l.merged_into_id IS NULL
               GROUP BY l.status"""
        )
        stats["leads_by_status"] = {row["status"]: row["count"] for row in await cursor.fetchall()}

        return stats
