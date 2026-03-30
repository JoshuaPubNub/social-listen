from __future__ import annotations

from social_listen.database import Database

SEED_KEYWORDS: dict[str, list[str]] = {
    "core": [
        "AI agents",
        "autonomous agents",
        "agentic AI",
        "AI agent platform",
        "agent orchestration",
        "multi-agent",
        "multi-agent system",
    ],
    "protocol": [
        "MCP",
        "Model Context Protocol",
        "A2A",
        "agent-to-agent",
        "tool use LLM",
        "function calling",
    ],
    "framework": [
        "CrewAI",
        "AutoGPT",
        "LangChain agents",
        "LangGraph",
        "AutoGen",
        "OpenAI Swarm",
        "smolagents",
    ],
    "concept": [
        "LLM agents",
        "agent framework",
        "agent memory",
        "agentic workflow",
        "agentic RAG",
        "ReAct agent",
        "agent tool use",
    ],
}


class KeywordManager:
    def __init__(self, db: Database):
        self.db = db

    async def seed(self) -> None:
        """Seed the database with initial keywords (idempotent)."""
        await self.db.seed_keywords(SEED_KEYWORDS)

    async def get_active_keywords(self) -> list[str]:
        """Get all active keyword terms."""
        rows = await self.db.get_active_keywords()
        return [row["term"] for row in rows]

    async def get_keywords_by_category(self) -> dict[str, list[str]]:
        """Get active keywords grouped by category."""
        rows = await self.db.get_active_keywords()
        result: dict[str, list[str]] = {}
        for row in rows:
            cat = row["category"] or "uncategorized"
            result.setdefault(cat, []).append(row["term"])
        return result

    async def get_search_queries_for_platform(self, platform: str) -> list[str]:
        """Get optimized search queries per platform.

        YouTube: combine keywords with OR to conserve quota.
        Twitter/Reddit: return individual keywords.
        """
        keywords = await self.get_active_keywords()

        if platform == "youtube":
            # Batch into groups of 3-4 keywords joined with OR
            queries = []
            for i in range(0, len(keywords), 3):
                batch = keywords[i : i + 3]
                queries.append(" | ".join(f'"{kw}"' for kw in batch))
            return queries

        return keywords
