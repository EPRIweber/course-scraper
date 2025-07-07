# src/main.py
import asyncio
import json
import logging, logging.config
import os
from typing import Optional

from src.config import SourceConfig, Stage, config, ValidationCheck
from src.crawler import close_playwright, crawl_and_collect_urls
from src.models import SourceRunResult
from src.prefilter import prefilter_urls
from src.prompts.taxonomy import load_full_taxonomy
from src.schema_manager import generate_schema, validate_schema
from src.scraper import scrape_urls
from src.classify_manager import classify_courses, flatten_taxonomy
from src.storage import SqlServerStorage, StorageBackend

LOGGING: dict = {
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

async def get_storage_backend() -> Optional[StorageBackend]:
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
        
async def process_schema(run_id: int, source: SourceConfig, storage: StorageBackend) -> None:
    stage: Stage = Stage.SCHEMA

    async def _log(st: Stage, msd: str):
        logger.info(f"[{source.name}] {msd}")
        await storage.log(run_id, source.source_id, int(st), msd)
    
    await _log(stage, f"RUNNING PROCESS_SCHEMA FOR {source.name}")

    try:
        # await _log(stage, "fetching / generating schema")
        schema = await storage.get_schema(source.source_id)
        # schema = None
        if (not schema) or (not schema.get("baseSelector")):
            schema, usage = await generate_schema(source)
            await _log(stage, f"generated schema with {usage} tokens")
            check: ValidationCheck = await validate_schema(
                schema=schema,
                source=source
            )
            if check.valid:
                await _log(stage, "successfully validated generated schema")
                await storage.save_schema(source.source_id, schema)
            else:
                await _log(stage, "ERROR: Invalid schema generated")
                if check.fields_missing:
                    await _log(stage, "Fields Missing: \n" + '\n'.join(
                        '- ' + field for field in check.fields_missing
                    ))
                if check.errors:
                    errors_joined = "\n\n\n".join(check.errors)
                    await _log(stage, f"Validation errors: \n{errors_joined}")
                return
        await _log(stage, "schema ready")
    except Exception as exc:
        await _log(stage, f"FAILED: {exc}")
        logger.exception(exc)

async def process_crawl(run_id: int, source: SourceConfig, storage: StorageBackend) -> None:
    stage: Stage = Stage.CRAWL

    async def _log(st: Stage, msd: str):
        logger.info(f"[{source.name}] {msd}")
        await storage.log(run_id, source.source_id, int(st), msd)
    
    await _log(stage, F"RUNNING PROCESS_CRAWL FOR {source.name}")

    try:
        urls = await storage.get_urls(source.source_id)
        if not urls:
            crawled = await crawl_and_collect_urls(source)
            # filtered = await prefilter_urls(crawled, source)
            filtered = crawled
            if not filtered:
                await _log(stage, f"ERROR: No URLs found after crawling and filtering")
                return
            await storage.save_urls(source.source_id, filtered)
        await _log(stage, f"{len(urls)} urls ready")
    except Exception as exc:
        await _log(stage, f"FAILED: {exc}")
        logger.exception(exc)

async def process_scrape(run_id: int, source: SourceConfig, storage: StorageBackend) -> Optional[SourceRunResult]:
    stage: Stage = Stage.SCRAPE

    async def _log(st: Stage, msg: str):
        logger.info(f"[{source.name}] {msg}")
        await storage.log(run_id, source.source_id, int(st), msg)

    try:
        urls = await storage.get_urls(source.source_id)
        if not urls:
            await _log(stage, "ERROR: Attempting to scrape without URLs")
            return None
        schema = await storage.get_schema(source.source_id)
        if not schema:
            await _log(stage, "ERROR: Attempting to scrape without Schema")
            return None

        good_urls, bad_urls = [], []
        await _log(stage, f"attempting to get data...")
        records = await storage.get_data(source.source_id)
        if not records:
            await _log(stage, f"no data found, scraping {len(urls)} pages")
            records, good_urls, bad_urls, json_errors = await scrape_urls(urls, schema, source)
            if json_errors:
                joined_json_errors = "\n\n\n".join(json_errors)
                await _log(stage, f"WARNING: Found {len(json_errors)} JSON errors: \n{joined_json_errors}")
            if not records:
                await _log(stage, "ERROR: No records extracted from pages")
                joined_json_errors = "\n\n\n".join(json_errors)
                await _log(stage, f"WARNING: Found {len(json_errors)} JSON errors: \n{joined_json_errors}")
                return
            await _log(stage, f"{len(records)} records scraped")

        
        # LOCAL RECORDS SAVE
        # records_file = "output_records.json"
        # good_urls_file = "good_urls.json"
        # bad_urls_file = "bad_urls.json"
        # try:
        #     with open(records_file, 'w') as f:
        #         json.dump(records, f, indent=2)
        #     print(f"Successfully wrote data to {records_file}")
        #     with open(good_urls_file, 'w') as f:
        #         json.dump(list(good_urls), f, indent=2)
        #     print(f"Successfully wrote data to {good_urls_file}")
        #     with open(bad_urls_file, 'w') as f:
        #         json.dump(list(bad_urls), f, indent=2)
        #     print(f"Successfully wrote data to {bad_urls_file}")
        # except IOError as e:
        #     print(f"Error writing to file: {e}")


        # -------- STORAGE -----------------------------------------------
        stage = Stage.STORAGE
        await _log(stage, "writing records to DB")
        # Ensure records is always a list of dicts
        if isinstance(records, dict):
            records = [records]
        await storage.save_data(source.source_id, records)
        if hasattr(storage, "update_url_targets"):
            # Ensure good_urls and bad_urls are lists of strings
            if not isinstance(good_urls, list):
                good_urls = list(good_urls) if good_urls else []
            if not isinstance(bad_urls, list):
                bad_urls = list(bad_urls) if bad_urls else []
            await storage.update_url_targets(
                source_id=source.source_id,
                good_urls=good_urls,
                bad_urls=bad_urls
            )
        await _log(stage, "done")
    except Exception as exc:
        await _log(stage, f"FAILED: {exc}")
        logger.exception(exc)

async def process_classify(run_id: int, source: SourceConfig, storage: StorageBackend) -> Optional[SourceRunResult]:
    stage: Stage = Stage.CLASSIFY

    async def _log(st: Stage, msg: str):
        logger.info(f"[{source.name}] {msg}")
        await storage.log(run_id, source.source_id, int(st), msg)

    try:
        records = await storage.get_data(source.source_id)
        if not records:
            await _log(stage, "No records to classify; skipping classification.")
            return

        # 2) prepare tuples for classification: (id, title, description)
        courses = [
            (
                rec.get("course_id") or "",
                rec.get("course_title") or "",
                rec.get("course_description") or ""
            )
            for rec in records
        ]

        # 3) run classification
        classified, usage = await classify_courses(courses)
        await _log(stage, f"Classified {len(classified)} courses using {usage} tokens")

        taxonomy_tree = load_full_taxonomy("taxonomy.json")
        valid_ids = flatten_taxonomy(taxonomy_tree)

        cleaned: list[tuple[str,list[str]]] = []
        empty: list[tuple[str,list[str]]] = []
        invalid: list[tuple[str,list[str]]] = []
        for course_id, labels in classified:
            good = []
            bad  = []
            for tid in labels:
                if tid in valid_ids:
                    good.append(tid)
                else:
                    bad.append(tid)
            if bad:
                invalid.append((course_id, bad))
                await _log(stage, f"Dropping invalid taxonomy IDs for {course_id}: {bad}")
            if good:
                cleaned.append((course_id, good))
            else:
                empty.append((course_id, []))

        await storage.save_classified(cleaned)
        
        
        # LOCAL RECORDS SAVE
        invalid_file = "invalid.json"
        empty_file = "empty.json"
        # clean_file = "clean.json"
        try:
            with open(invalid_file, 'w') as f:
                json.dump(invalid, f, indent=2)
            print(f"Successfully wrote data to {invalid_file}")
            with open(empty_file, 'w') as f:
                json.dump(list(empty), f, indent=2)
            print(f"Successfully wrote data to {empty_file}")
            
        except IOError as e:
            print(f"Error writing to file: {e}")

        
    except Exception as exc:
        await _log(stage, f"FAILED: {exc}")
        logger.exception(exc)

async def main():
    storage = await get_storage_backend()
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
        # 1.  Pull sources from DB and local YAML file
        sources = await storage.list_sources()
        if not sources:
            logger.warning("No sources found; skipping all processing tasks.")
            return

        # 2.  Kick off scraping tasks
        # tasks = [process_schema(run_id, src, storage) for src in sources]
        # await asyncio.gather(*tasks)
        # tasks = [process_crawl(run_id, src, storage) for src in sources]
        # await asyncio.gather(*tasks)
        # tasks = [process_scrape(run_id, src, storage) for src in sources]
        # await asyncio.gather(*tasks)
        tasks = [process_classify(run_id, src, storage) for src in sources]
        await asyncio.gather(*tasks)

        
        # await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as exc:
        logger.exception("Critical error in run %d: %s", run_id, exc)

    finally:
        await storage.end_run(run_id)               # unlock mutex
        logger.info("Run %d completed – lock released.", run_id)
        await close_playwright()

async def testing():
    test_source = config.sources[0]
    print("generating test schema...")
    schema, usage = await generate_schema(test_source)
    print(schema)

if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(testing())