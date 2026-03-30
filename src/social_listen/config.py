from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class TwitterConfig(BaseModel):
    enabled: bool = True
    interval_minutes: int = 60
    max_pages_per_keyword: int = 3
    request_delay_seconds: int = 3
    follower_threshold: int = 1000


class RedditConfig(BaseModel):
    enabled: bool = True
    interval_minutes: int = 30
    subreddits: list[str] = [
        "artificial", "MachineLearning", "LocalLLaMA", "LLMDevs",
        "AutoGPT", "LangChain", "ChatGPTCoding", "singularity", "OpenAI",
    ]
    posts_per_keyword: int = 25
    karma_threshold: int = 1000


class YouTubeConfig(BaseModel):
    enabled: bool = True
    interval_hours: int = 6
    daily_unit_budget: int = 10000
    subscriber_threshold: int = 1000
    max_results_per_search: int = 25


class CollectorsConfig(BaseModel):
    twitter: TwitterConfig = TwitterConfig()
    reddit: RedditConfig = RedditConfig()
    youtube: YouTubeConfig = YouTubeConfig()


class ScoringWeights(BaseModel):
    audience: float = 0.40
    relevance: float = 0.35
    engagement: float = 0.15
    recency: float = 0.10


class ScoringConfig(BaseModel):
    weights: ScoringWeights = ScoringWeights()


class DashboardConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    page_size: int = 50


class DatabaseConfig(BaseModel):
    path: str = "./data/social_listen.db"


class AppConfig(BaseModel):
    database: DatabaseConfig = DatabaseConfig()
    collectors: CollectorsConfig = CollectorsConfig()
    scoring: ScoringConfig = ScoringConfig()
    dashboard: DashboardConfig = DashboardConfig()


class EnvSettings(BaseSettings):
    twitter_bearer_token: str = ""
    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = "social_listen:v1.0"
    youtube_api_key: str = ""
    database_path: str = ""
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


def _expand_env_vars(value: str) -> str:
    """Expand ${VAR:-default} patterns in config strings."""
    if "${" not in value:
        return value
    import re
    def _replace(match: re.Match) -> str:
        var_name = match.group(1)
        default = match.group(2) or ""
        return os.environ.get(var_name, default)
    return re.sub(r"\$\{(\w+)(?::-([^}]*))?\}", _replace, value)


def load_config(config_path: str = "config.yml") -> AppConfig:
    """Load configuration from YAML file, expanding env vars."""
    path = Path(config_path)
    if path.exists():
        with open(path) as f:
            raw = yaml.safe_load(f) or {}

        # Expand env vars in database path
        if "database" in raw and "path" in raw["database"]:
            raw["database"]["path"] = _expand_env_vars(raw["database"]["path"])

        return AppConfig(**raw)
    return AppConfig()


def load_env() -> EnvSettings:
    """Load environment variables / .env file."""
    return EnvSettings()
