from __future__ import annotations

import logging

from fastapi import FastAPI

from social_listen.config import AppConfig, EnvSettings
from social_listen.database import Database
from social_listen.engine.keywords import KeywordManager

logger = logging.getLogger(__name__)


def create_app(config: AppConfig, env: EnvSettings) -> FastAPI:
    app = FastAPI(title="Social Listen", docs_url="/api/docs")

    db = Database(config.database.path)

    @app.on_event("startup")
    async def startup() -> None:
        await db.connect()
        await db.initialize()

        # Seed keywords
        km = KeywordManager(db)
        await km.seed()
        logger.info("Database initialized and keywords seeded")

        # Start scheduler
        from social_listen.scheduler.runner import CollectionScheduler
        scheduler = CollectionScheduler(config, env, db)
        scheduler.start()
        app.state.scheduler = scheduler
        logger.info("Collection scheduler started")

    @app.on_event("shutdown")
    async def shutdown() -> None:
        if hasattr(app.state, "scheduler"):
            app.state.scheduler.stop()
        await db.close()

    # Store db and config on app state for route access
    app.state.db = db
    app.state.config = config
    app.state.env = env

    # Register routes
    from social_listen.dashboard.routes import create_router
    app.include_router(create_router())

    return app
