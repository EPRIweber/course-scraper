# src/main.py
"""Application entry point.

This module orchestrates the entire scraping pipeline. It retrieves enabled
sources from the database, runs the crawl and scrape stages and finally stores
results.  Modify this file if you need to change the overall workflow.
"""

import asyncio
import csv
import json
import logging, logging.config
import os
from typing import Optional

from src.config import SourceConfig, Stage, config, ValidationCheck
from src.config_generator import discover_source_config
from src.crawler import crawl_and_collect_urls
from src.render_utils import close_playwright
from src.models import SourceRunResult
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
            "MARS_Connection=Yes;"
        )
        logger.info("Using SQL-Server storage backend")
        return SqlServerStorage(conn_str)
    except Exception as exc:
        logger.exception(exc)
        raise Exception(exc)
        
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
            check, output = await validate_schema(
                schema=schema,
                source=source
            )
            check: ValidationCheck
            await _log(stage, output)
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
                raise Exception(f"Invalid schema generated for {source.name}")
        else:
            await _log(stage, f"Schema already created for {source.name}")
    except Exception as exc:
        await _log(stage, f"FAILED: {exc}")
        logger.exception(exc)
        raise Exception(exc)

async def process_test_schema(run_id: int, source: SourceConfig, storage: StorageBackend) -> None:
    stage: Stage = Stage.SCHEMA

    async def _log(st: Stage, msd: str):
        logger.info(f"[{source.name}] {msd}")
        await storage.log(run_id, source.source_id, int(st), msd)
    
    await _log(stage, f"RUNNING PROCESS_TEST_SCHEMA FOR {source.name}")

    try:
        # await _log(stage, "fetching / generating schema")
        schema = await storage.get_schema(source.source_id)
        # schema = None
        if (not schema) or (not schema.get("baseSelector")):
            await _log(stage, "No schema found")
            return
        else:
            await _log(stage, f"Testing schema for {source.name}")
            check: ValidationCheck = await validate_schema(
                schema=schema,
                source=source
            )
            if check.valid:
                await _log(stage, "successfully validated schema")
            else:
                await _log(stage, "ERROR: Invalid schema")
                if check.fields_missing:
                    await _log(stage, "Fields Missing: \n" + '\n'.join(
                        '- ' + field for field in check.fields_missing
                    ))
                if check.errors:
                    errors_joined = "\n\n\n".join(check.errors)
                    await _log(stage, f"Validation errors: \n{errors_joined}")
                return
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
                raise Exception(f"Failed crawling {source.name}")
            await storage.save_urls(source.source_id, filtered)
            await _log(stage, f"{len(filtered)} urls ready")
        else:
            await _log(stage, f"{len(urls)} urls retrieved from database")
    except Exception as exc:
        await _log(stage, f"FAILED: {exc}")
        logger.exception(exc)
        raise Exception(f"Failed crawling {source.name}")

async def process_scrape(run_id: int, source: SourceConfig, storage: StorageBackend) -> Optional[SourceRunResult]:
    stage: Stage = Stage.SCRAPE

    async def _log(st: Stage, msg: str):
        logger.info(f"[{source.name}] {msg}")
        await storage.log(run_id, source.source_id, int(st), msg)

    data = await storage.get_data(source.source_id)
    if not data:
        try:
            urls = await storage.get_urls(source.source_id)
            if not urls:
                await _log(stage, "ERROR: Attempting to scrape without URLs")
                raise Exception(f"ERROR: Attempting to scrape without URLs for {source.name}")
            schema = await storage.get_schema(source.source_id)
            if not schema:
                await _log(stage, "ERROR: Attempting to scrape without Schema")
                raise Exception(f"ERROR: Attempting to scrape without schema for {source.name}")
            
            # with open("src/modern_campus.json", 'r') as f:
            #     modern_campus_schema = json.load(f)
            # if schema == modern_campus_schema:
            #     await _log(stage, "Skipping modern campus schema")
            #     return None

            good_urls, bad_urls = [], []
            await _log(stage, f"attempting to get data...")
            records = await storage.get_data(source.source_id)
            if not records:
                await _log(stage, f"no data found, scraping {len(urls)} pages")
                records, good_urls, bad_urls, result_errors = await scrape_urls(urls, schema, source)
                if result_errors:
                    joined_result_errors = "\n\n\n".join(result_errors)
                    await _log(stage, f"WARNING: Found {len(result_errors)} errors: \n{joined_result_errors}")
                if not records:
                    await _log(stage, "ERROR: No records extracted from pages")
                    joined_result_errors = "\n\n\n".join(result_errors)
                    await _log(stage, f"WARNING: Found {len(result_errors)} errors: \n{joined_result_errors}")
                    raise Exception(f"WARNING: Found {len(result_errors)} errors: \n{joined_result_errors}\n\n for {source.name}")
                await _log(stage, f"{len(records)} records scraped")

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
            raise Exception(f"Failure: {exc}\n\n for {source.name}")
    else:
        await _log(stage, f"Data already exists for {source.name}")

async def process_classify(run_id: int, source: SourceConfig, storage: StorageBackend) -> Optional[SourceRunResult]:
    stage: Stage = Stage.CLASSIFY

    async def _log(st: Stage, msg: str):
        logger.info(f"[{source.name}] {msg}")
        await storage.log(run_id, source.source_id, int(st), msg)

    try:
        classified = await storage.get_classified(source.source_id)
        if not classified:
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

            taxonomy_tree = load_full_taxonomy()
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
                    await _log(stage, f"Dropping invalid taxonomy IDs for {source.name}: {bad}")
                if good:
                    cleaned.append((course_id, good))
                else:
                    empty.append((course_id, []))

            await storage.save_classified(cleaned)
            
            
            # LOCAL RECORDS SAVE
            # invalid_file = "invalid.json"
            # empty_file = "empty.json"
            # # clean_file = "clean.json"
            # try:
            #     with open(invalid_file, 'w') as f:
            #         json.dump(invalid, f, indent=2)
            #     print(f"Successfully wrote data to {invalid_file}")
            #     with open(empty_file, 'w') as f:
            #         json.dump(list(empty), f, indent=2)
            #     print(f"Successfully wrote data to {empty_file}")
                
            # except IOError as e:
            #     print(f"Error writing to file: {e}")
        else:
            await _log(stage, f"found {len(classified)} classification records in database")
        
    except Exception as exc:
        await _log(stage, f"FAILED: {exc}")
        logger.exception(exc)

# -----------------------------------------------------------------------------
# Orchestration for a single school
# -----------------------------------------------------------------------------
async def run_scrape_pipeline(
    school: str,
    run_id: int,
    storage: StorageBackend,
) -> None:
    """Generate config, upsert source, then run schema→crawl→scrape→classify."""
    async def _log(source_id: str, st: Stage, msg: str):
        logger.info(f"[{school}] {msg}")
        await storage.log(run_id, source_id, int(st), msg)
    try:
        await storage.log(
            run_id=run_id,
            src_id=None,
            stage=Stage.CRAWL,
            msg=f"Generating Source for {school}"
        )
        src_cfg, root_usage, schema_usage = await discover_source_config(school)
        src_cfg: SourceConfig
        await storage.log(
            run_id=run_id,
            src_id=None,
            stage=Stage.CRAWL,
            msg=f"Source Generated for {school}"
        )
    except Exception as e:
        await storage.log(
            run_id=run_id,
            src_id=None,
            stage=Stage.CRAWL,
            msg=f"Config generation failed for {school}: {e}"
        )
        raise Exception(f"Config generation failed for {school}: {e}")
    try:
        real_id = await storage.ensure_source(src_cfg)
        src_cfg.source_id = real_id
    except Exception as e:
        await storage.log(
            run_id=run_id,
            src_id=None,
            stage=Stage.CRAWL,
            msg=f"Failed to upsert source {school}: {e}"
        )
        raise Exception(f"Failed to upsert source {school}: {e}")
        
    await _log(real_id, Stage.CRAWL, f"Created source config for {str(school)} using {root_usage} tokens for root URL and {schema_usage} tokens for schema URL")
    await _log(real_id, Stage.STORAGE, f"Beginning pipeline for {school}")
    
    await process_schema(run_id, src_cfg, storage)
    # await process_crawl(run_id, src_cfg, storage)
    # await process_scrape(run_id, src_cfg, storage)
    # await process_classify(run_id, src_cfg, storage)
    # logger.info("Completed pipeline for %s", school)
    await _log(real_id, Stage.STORAGE, f"Completed pipeline for {school}")

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

    MAX_CONCURRENT = 10
    sem = asyncio.BoundedSemaphore(MAX_CONCURRENT)
    async def limited_run(school: str, run_id: int, storage: StorageBackend):
        async with sem:
            await storage.log(
                run_id=run_id,
                src_id=None,
                stage=Stage.CRAWL,
                msg=(f"[{school}] starting (slots left: {sem._value})")
            )
            return await run_scrape_pipeline(school, run_id, storage)

    try:
        new_schools = []
        with open('configs/new_schools.csv', 'r') as f:
            csv_reader = csv.reader(f)
            for r in csv_reader:
                new_schools.append(r[0])

        try:
            tasks = [
                limited_run(school, run_id, storage)
                for school in new_schools
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for school, result in zip(new_schools, results):
                if isinstance(result, Exception):
                    logger.error(f"Pipeline for {school} FAILED: {result}")
                    await storage.log(
                        run_id=run_id,
                        src_id=None,
                        stage=Stage.CRAWL,
                        msg=f"Pipeline for {school} FAILED: {result}"
                    )
                else:
                    print(f"Task succeeded with result: {result}")

        except Exception as exc:
            await storage.log(
                run_id=run_id,
                src_id=None,
                stage=Stage.CRAWL,
                msg=f"Pipeline Fail: {exc}"
            )
            raise Exception(exc)

    except Exception as exc:
        logger.exception("Critical error in run %d: %s", run_id, exc)

    finally:
        await storage.end_run(run_id)               # unlock mutex
        logger.info("Run %d completed – lock released.", run_id)
        await close_playwright()

async def testing():
    # test_source = config.sources[0]
    # print(f"generating test schema for {test_source.name}")
    # schema = None
    # print(schema)
    # check: ValidationCheck = await validate_schema(
    #     schema=schema,
    #     source=test_source
    # )


    # urls = await crawl_and_collect_urls(test_source)
    # print(urls)

    src_cfg, root_usage, schema_usage = await discover_source_config("oregon state university")

    print(
f"""Source Config:\n{src_cfg}"""
    )

if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(testing())