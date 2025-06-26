# src/main.py
import asyncio
import logging, logging.config
from logging.config import dictConfigClass
import os

from src.config import SourceConfig, Stage, config, ValidationCheck
from src.crawler import crawl_and_collect_urls
from src.models import SourceRunResult
from src.prefilter import prefilter_urls
from src.schema_manager import generate_schema, validate_schema
from src.scraper import scrape_urls
from src.storage import SqlServerStorage, StorageBackend

LOGGING: dictConfigClass  = {
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

# Remove master handler to remove local storage
LOGGING["handlers"].pop("master", None)
LOGGING["loggers"][""]["handlers"] = ["console"]

logging.config.dictConfig(LOGGING)
# Suppress noisy library logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def get_storage_backend() -> StorageBackend:
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASS')};"
            "TrustServerCertificate=yes;"
            "Encrypt=yes;"
        )
        logger.info("Using SQL-Server storage backend")
        return SqlServerStorage(conn_str)
    except Exception as exc:
        logger.exception(exc)
        return None

async def process_source(run_id: int, source: SourceConfig, storage: StorageBackend) -> SourceRunResult | None:
    source_id = await storage.ensure_source(source)
    stage  = Stage.CRAWL

    async def _log(st: Stage, msg: str):
        logger.info(f"[{source.name}] {msg}")
        await storage.log(run_id, source_id, int(st), msg)

    try:
        # -------- CRAWL -------------------------------------------------
        stage = Stage.CRAWL
        await _log(stage, "starting crawl")
        urls = await storage.get_urls(source_id)
        if not urls:
            crawled = await crawl_and_collect_urls(source)
            filtered = await prefilter_urls(crawled, source)
            await storage.save_urls(source_id, filtered)
            urls = filtered
            if not urls:
                _log(stage, f"ERROR: No URLs found after crawling and filtering")
                return
        await _log(stage, f"{len(urls)} urls ready")

        # -------- SCHEMA ------------------------------------------------
        stage = Stage.SCHEMA
        await _log(stage, "fetching / generating schema")
        schema = await storage.get_schema(source_id)
        if not schema.get("baseSelector"):
            schema, usage = await generate_schema(source)
            await _log(stage, f"generated schema with {usage} tokens")
            check: ValidationCheck = await validate_schema(
                schema=schema,
                source=source
            )
            if check.valid:
                await _log(stage, "successfully validated generated schema")
                await storage.save_schema(source_id, schema)
            else:
                _log(stage, "ERROR: Invalid schema generated")
                if check.fields_missing:
                    _log(stage, f"Fields Missing: \n{"\n".join(
                        "- " + field for field in check.fields_missing
                    )}")
                if check.errors:
                    _log(stage, f"Validation errors: \n{"\n\n\n".join(check.errors)}")
                return
        await _log(stage, "schema ready")

        # -------- SCRAPE ------------------------------------------------
        stage = Stage.SCRAPE
        good_urls, bad_urls = [], []
        await _log(stage, f"attempting to get data...")
        records = await storage.get_data(source_id)
        if not records:
            await _log(stage, f"no data found, scraping {len(urls)} pages")
            records, good_urls, bad_urls, json_errors = await scrape_urls(urls, schema, source)
            if json_errors:
                await _log(stage, f"WARNING: Found {len(json_errors)} JSON errors: \n{"\n\n\n".join(json_errors)}")
            if not records:
                await _log(stage, "ERROR: No records extracted from pages")
                await _log(stage, f"ERROR: Encountered {len(json_errors)} JSON errors: \n{"\n\n\n".join(json_errors)}")
                return
            await _log(stage, f"{len(records)} records scraped")

        # -------- STORAGE -----------------------------------------------
        stage = Stage.STORAGE
        await _log(stage, "writing records to DB")
        await storage.save_data(source_id, records)
        if hasattr(storage, "update_url_targets"):
            await storage.update_url_targets(
                source_id=source_id,
                good_urls=good_urls,
                bad_urls=bad_urls
            )
        await _log(stage, "done")

        # result.status = "success"

    except Exception as exc:
        await _log(stage, f"FAILED: {exc}")
        logger.exception(exc)
        # result.status = "failure"

    # return result

async def main():
    storage = get_storage_backend()
    if storage is None:
        logger.critical("SQL credentials missing – aborting.")
        return
    
    # --- mutex -----------------------------------------------------------
    try:
        run_id = await storage.begin_run()          # atomic lock
    except RuntimeError as e:
        logger.error(str(e))
        return

    logger.info("Run ID: %d", run_id)

    try:
        # 1.  Pull sources from DB; if table is empty, fall back to YAML list
        sources = await storage.list_sources()
        if not sources:
            logger.warning("No sources in DB – falling back to YAML config.")
            sources = config.sources

        # 2.  Kick off scraping tasks
        tasks = [process_source(run_id, src, storage) for src in sources]
        await asyncio.gather(*tasks)                # storage passed inside

    except Exception as exc:
        logger.exception("Critical error in run %d: %s", run_id, exc)

    finally:
        await storage.end_run(run_id)               # unlock mutex
        logger.info("Run %d completed – lock released.", run_id)

async def testing():
    test_source = config.sources[0]
    print("generating test schema...")
    schema, usage = await generate_schema(test_source)
    print(schema)

if __name__ == "__main__":
    # asyncio.run(main())
    asyncio.run(testing())