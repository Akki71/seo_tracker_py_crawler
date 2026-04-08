#!/usr/bin/env python3
"""
startup.py — Initialize PostgreSQL schema and start FastAPI.
Called by the container entrypoint.
"""
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def main():
    logger.info("Initializing PostgreSQL schema...")
    try:
        from db import init_db
        init_db()
        logger.info("Schema ready.")
    except Exception as e:
        logger.error(f"DB init failed: {e}")
        sys.exit(1)

    logger.info("Starting FastAPI server...")
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        workers=2,
        log_level="info",
    )

if __name__ == "__main__":
    main()
