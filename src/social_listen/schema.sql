-- Enable WAL mode for concurrent read/write
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;

-- A lead is a unique person. They may have accounts on multiple platforms.
CREATE TABLE IF NOT EXISTS leads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    display_name    TEXT,
    notes           TEXT,
    status          TEXT DEFAULT 'new' CHECK(status IN ('new', 'contacted', 'replied', 'ignored')),
    lead_score      REAL DEFAULT 0.0,
    first_seen_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    merged_into_id  INTEGER REFERENCES leads(id)
);

-- One row per platform identity. A lead can have 0-3 of these.
CREATE TABLE IF NOT EXISTS platform_accounts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id         INTEGER NOT NULL REFERENCES leads(id),
    platform        TEXT NOT NULL CHECK(platform IN ('twitter', 'reddit', 'youtube')),
    platform_user_id TEXT NOT NULL,
    username        TEXT,
    profile_url     TEXT,
    display_name    TEXT,
    bio             TEXT,
    follower_count  INTEGER DEFAULT 0,
    following_count INTEGER DEFAULT 0,
    avatar_url      TEXT,
    raw_data        TEXT,
    last_checked_at TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, platform_user_id)
);

-- Every matching post/tweet/comment/video we find.
CREATE TABLE IF NOT EXISTS posts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    platform_account_id INTEGER NOT NULL REFERENCES platform_accounts(id),
    platform            TEXT NOT NULL,
    platform_post_id    TEXT NOT NULL,
    content             TEXT,
    url                 TEXT,
    post_type           TEXT CHECK(post_type IN ('tweet', 'reddit_post', 'reddit_comment', 'video')),
    engagement          TEXT,
    relevance_score     REAL DEFAULT 0.0,
    matched_keywords    TEXT,
    posted_at           TIMESTAMP,
    discovered_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, platform_post_id)
);

-- Track collection runs for observability.
CREATE TABLE IF NOT EXISTS collection_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    collector       TEXT NOT NULL,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    status          TEXT DEFAULT 'running' CHECK(status IN ('running', 'completed', 'failed')),
    posts_found     INTEGER DEFAULT 0,
    leads_created   INTEGER DEFAULT 0,
    error_message   TEXT,
    metadata        TEXT
);

-- Keyword configuration stored in DB for runtime editing via dashboard.
CREATE TABLE IF NOT EXISTS keywords (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    term            TEXT NOT NULL UNIQUE,
    category        TEXT CHECK(category IN ('core', 'protocol', 'framework', 'concept')),
    is_active       INTEGER DEFAULT 1,
    added_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_pa_platform ON platform_accounts(platform);
CREATE INDEX IF NOT EXISTS idx_pa_lead ON platform_accounts(lead_id);
CREATE INDEX IF NOT EXISTS idx_posts_pa ON posts(platform_account_id);
CREATE INDEX IF NOT EXISTS idx_posts_discovered ON posts(discovered_at);
CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(lead_score DESC);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_merged ON leads(merged_into_id);
