# src/main.py
import asyncio
import logging
import os
from pathlib import Path

from src.config import SourceConfig, Stage, config
from src.crawler import crawl_and_collect_urls
from src.models import ErrorLog, JobSummary, SourceRunResult
from src.prefilter import prefilter_urls
from src.schema_manager import generate_schema
from src.scraper import scrape_urls
from src.storage import LocalFileStorage, FirestoreStorage, SqlServerStorage, StorageBackend

LOGGING = {
  "version": 1,
  "disable_existing_loggers": False,
  "formatters": {
    "default": {"format": "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"},
  },
  "handlers": {
    "console": {"class": "logging.StreamHandler", "formatter": "default"},
    "master": {
      "class": "logging.handlers.RotatingFileHandler",
      "maxBytes": 5_000_000, "backupCount": 3,
      "formatter": "default",
    },
  },
  "loggers": {
    "": {"handlers": ["console", "master"], "level": "INFO"},
  },
}

logging.config.dictConfig(LOGGING)
# Suppress noisy library logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def get_storage_backend() -> StorageBackend:
    if os.getenv("DB_SERVER"):
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};PWD={os.getenv('DB_PASS')};"
            "Encrypt=yes;TrustServerCertificate=yes;"
        )
        logger.info("Using SQL-Server storage backend")
        return SqlServerStorage(conn_str)
    elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logger.info("Using Firestore storage backend")
        return FirestoreStorage()
    else:
        logger.info("Using LocalFileStorage backend")
        return LocalFileStorage(Path("data"))

storage = get_storage_backend()

async def process_source(run_id: str, source: SourceConfig) -> SourceRunResult:
    result = SourceRunResult(source_name=source.name, status="in-progress")
    src_id = await storage.ensure_source(source)
    stage  = Stage.CRAWL

    async def _log(st: Stage, msg: str):
        logger.info(f"[{source.name}] {msg}")
        await storage.log(run_id, src_id, int(st), msg)

    try:
        # -------- CRAWL -------------------------------------------------
        stage = Stage.CRAWL
        await _log(stage, "starting crawl")
        urls = await storage.get_urls(source.name)
        if not urls:
            crawled = await crawl_and_collect_urls(source)
            filtered = await prefilter_urls(crawled, source)
            await storage.save_urls(src_id, filtered)
            urls = filtered
        await _log(stage, f"{len(urls)} urls ready")

        # -------- SCHEMA ------------------------------------------------
        stage = Stage.SCHEMA
        await _log(stage, "fetching / generating schema")
        schema = await storage.get_schema(source.name)
        if not schema.get("baseSelector"):
            schema, usage = await generate_schema(source)
            await storage.save_schema(src_id, schema)
            await _log(stage, f"generated schema with {usage} tokens")
        await _log(stage, "schema ready")

        # -------- SCRAPE ------------------------------------------------
        stage = Stage.SCRAPE
        await _log(stage, f"scraping {len(urls)} pages")
        records, good_urls, bad_urls = await scrape_urls(urls, schema, source)
        if not records:
            raise ValueError("No records scraped, check schema or URLs")
        await _log(stage, f"{len(records)} records scraped")

        # -------- STORAGE -----------------------------------------------
        stage = Stage.STORAGE
        await _log(stage, "writing records to DB")
        await storage.save_data(src_id, records)
        if hasattr(storage, "update_url_targets"):
            await storage.update_url_targets(
                src_id=src_id,
                good_urls=good_urls,
                bad_urls=bad_urls
            )
        await _log(stage, "done")

        result.status = "success"

    except Exception as exc:
        await _log(stage, f"FAILED: {exc}")
        logger.exception(exc)
        result.status = "failure"

    return result

async def main():
    run_id = await storage.new_run()
    logger.info(f"Run ID: {run_id}")

    tasks   = [process_source(run_id, s) for s in config.sources]
    await asyncio.gather(*tasks, return_exceptions=False) # Set return_exceptions=True to return errors instead of failing
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (ValueError, FileNotFoundError) as e:
        logger.critical(f"A critical configuration error occurred: {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred in main execution: {e}", exc_info=True)
