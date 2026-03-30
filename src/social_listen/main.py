from __future__ import annotations

import logging
import os

import uvicorn

from social_listen.config import load_config, load_env


def main() -> None:
    env = load_env()
    logging.basicConfig(
        level=getattr(logging, env.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = load_config()

    # Override DB path from env if set
    if env.database_path:
        config.database.path = env.database_path

    # Railway (and other PaaS) set PORT env var
    port = int(os.environ.get("PORT", config.dashboard.port))

    from social_listen.dashboard.app import create_app
    app = create_app(config, env)

    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
