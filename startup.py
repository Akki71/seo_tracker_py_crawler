#!/usr/bin/env python3
"""
startup.py — Initialize PostgreSQL schema then start FastAPI.
DB init failure is logged but does NOT prevent the server from starting.
"""
import sys
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def init_db_safe():
    """Try to init DB schema. Non-fatal if it fails at startup."""
    try:
        from db import init_db
        init_db()
        logger.info("PostgreSQL schema initialized successfully.")
    except Exception as e:
        logger.error(f"DB init error (non-fatal, will retry on first request): {e}")


def main():
    logger.info("=" * 60)
    logger.info("AquilTechLabs SEO Crawler API v2.0")
    logger.info(f"Python: {sys.version}")
    logger.info(f"DB_HOST: {os.environ.get('DB_HOST', 'NOT SET')}")
    logger.info(f"DB_NAME: {os.environ.get('DB_NAME', 'NOT SET')}")
    logger.info(f"AI keys: OpenAI={'SET' if os.environ.get('OPENAI_API_KEY') else 'NOT SET'} | "
                f"Anthropic={'SET' if os.environ.get('ANTHROPIC_API_KEY') else 'NOT SET'}")
    logger.info("=" * 60)

    # Try DB init — but don't die if it fails
    init_db_safe()

    # Always start the API server
    logger.info("Starting uvicorn server on 0.0.0.0:8000 ...")
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        workers=1,          # 1 worker — jobs dict is in-memory, multi-worker needs Redis
        log_level="info",
        access_log=True,
    )


if __name__ == "__main__":
    main()
