# src/main.py

import asyncio
import logging, logging.config
from logging.config import dictConfigClass
import os
import traceback

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

# In src/main.py

async def process_source(run_id: str, source: SourceConfig, storage: StorageBackend) -> SourceRunResult | None:
    # TODO: Remove once source retrieved from database in main
    source_id = await storage.ensure_source(source)
    stage = Stage.CRAWL
    
    # Map Python's integer levels to the strings we'll store in the DB
    LOG_LEVEL_MAP = {
        logging.INFO: 'INFO',
        logging.WARNING: 'WARNING',
        logging.ERROR: 'ERROR',
        logging.CRITICAL: 'CRITICAL'
    }

    async def _log(st: Stage,
                   level: int,
                   event_type: str,
                   msg: str,
                   details: str = None,
                   metric_name: str = None,
                   metric_value: float = None):
        """Helper to send structured logs to both the console and the database."""
        str_level = LOG_LEVEL_MAP.get(level, 'INFO')
        
        # Log a simple message to the console for real-time monitoring
        console_msg = f"[{source.name}] {msg}"
        if details:
            console_msg += f" (see database for details)"
        logger.log(level, console_msg)

        # Send the full structured log to the database
        await storage.log(
            run_id=int(run_id), 
            src_id=source_id, 
            stage=int(st), 
            level=str_level, 
            event_type=event_type,
            msg=msg, 
            details=details, 
            metric_name=metric_name, 
            metric_value=metric_value
        )

    try:
        # -------- CRAWL -------------------------------------------------
        stage = Stage.CRAWL
        await _log(stage, logging.INFO, 'CrawlStarted', "Starting crawl")
        urls = await storage.get_urls(source_id)
        if not urls:
            crawled = await crawl_and_collect_urls(source)
            filtered = await prefilter_urls(crawled, source)
            await storage.save_urls(source_id, filtered)
            urls = filtered
            if not urls:
                await _log(stage, logging.ERROR, 'CrawlFailed', "No URLs found after crawling and filtering")
                return
        await _log(stage, logging.INFO, 'CrawlComplete', f"{len(urls)} URLs ready for processing", metric_name='UrlsFound', metric_value=len(urls))

        # -------- SCHEMA ------------------------------------------------
        stage = Stage.SCHEMA
        await _log(stage, logging.INFO, 'SchemaStarted', "Fetching / generating schema")
        schema = await storage.get_schema(source_id)
        if not schema.get("baseSelector"):
            schema, usage = await generate_schema(source)
            await _log(stage, logging.INFO, 'SchemaGenerated', f"Generated schema with {usage} tokens", metric_name='LlmTokenUsage', metric_value=usage)
            check: ValidationCheck = validate_schema(schema=schema, source=source)
            if check.valid:
                await storage.save_schema(source_id, schema)
            else:
                error_details = []
                if check.fields_missing:
                    error_details.append(f"Fields Missing: {', '.join(check.fields_missing)}")
                if check.errors:
                    error_details.append(f"Validation Errors: {', '.join(check.errors)}")
                await _log(stage, logging.ERROR, 'SchemaValidationError', "Invalid schema generated", details='\n'.join(error_details))
                return
        await _log(stage, logging.INFO, 'SchemaReady', "Schema is ready")

        # -------- SCRAPE ------------------------------------------------
        stage = Stage.SCRAPE
        await _log(stage, logging.INFO, 'ScrapeStarted', "Attempting to get data...")
        records = await storage.get_data(source_id)
        if not records:
            await _log(stage, logging.INFO, 'ScrapeInProgress', f"No data found in DB, scraping {len(urls)} pages")
            records, good_urls, bad_urls, json_errors = await scrape_urls(urls, schema, source)
            if json_errors:
                await _log(stage, logging.WARNING, 'ScrapeJsonErrors', f"Found {len(json_errors)} JSON errors",
                           details="\n\n\n".join(json_errors), metric_name='JsonErrorCount', metric_value=len(json_errors))
            if not records:
                await _log(stage, logging.ERROR, 'ScrapeFailed', "No records extracted from pages")
                return
            await _log(stage, logging.INFO, 'ScrapeComplete', f"{len(records)} records scraped",
                       metric_name='RecordsScraped', metric_value=len(records))

        # -------- STORAGE -----------------------------------------------
        stage = Stage.STORAGE
        await _log(stage, logging.INFO, 'StorageStarted', "Writing records to DB")
        await storage.save_data(source_id, records)
        if hasattr(storage, "update_url_targets"):
            await storage.update_url_targets(source_id=source_id, good_urls=good_urls, bad_urls=bad_urls)
        await _log(stage, logging.INFO, 'StorageComplete', "Done")

    except Exception as exc:
        # Catchall for any other failure
        await _log(stage, logging.CRITICAL, 'ProcessFailed', f"A critical error occurred in stage: {stage.name}", details=traceback.format_exc())
        logger.exception(exc) # Also log the full exception to the console

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

    logger.info("Run ID: %s", run_id)

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
        logger.exception("Critical error in run %s: %s", run_id, exc)

    finally:
        await storage.end_run(run_id)               # unlock mutex
        logger.info("Run %s completed – lock released.", run_id)


if __name__ == "__main__":
    asyncio.run(main())


#     # TODO: LOCK MUTEX, RETURNS RUN_ID (now will be identity key)
#     run_id = await storage.begin_run() # create mutex

#     try:
#         logger.info(f"Run ID: {run_id}")

#         # TODO: THIS IS WHERE SOURCES SHOULD BE PULLED FROM THE DATABASE
#         # EVENTUALLY, AUTO-GENERATE CONFIGS FOR SOURCES

#         tasks = [process_source(run_id, s) for s in config.sources]
#         await asyncio.gather(*tasks, return_exceptions=False, storage=storage) # Set return_exceptions=True to return errors instead of failing
#     except Exception as exc:
#         logger.critical(f"Critical error occurred: {exc}")
    
#     finally:
#         #TODO UNLOCK MUTEX
#         await storage.end_run(run_id)
    
# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except (ValueError, FileNotFoundError) as e:
#         logger.critical(f"A critical configuration error occurred: {e}", exc_info=True)
#     except Exception as e:
#         logger.critical(f"An unexpected critical error occurred in main execution: {e}", exc_info=True)
