# src/main.py
import asyncio
import logging
import os
from pathlib import Path
from datetime import datetime, timezone
import uuid

from src.config import SourceConfig, config
from src.crawler import crawl_and_collect_urls
from src.models import ErrorLog, JobSummary, SourceRunResult
from .reporting import generate_summary_report
from src.prefilter import prefilter_urls
from src.schema_manager import generate_schema
from src.scraper import scrape_urls
from src.storage import LocalFileStorage, FirestoreStorage, StorageBackend

# Setup centralized logging
log_file_path = Path("scraper.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file_path)
    ]
)
# Suppress noisy library logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def get_storage_backend() -> StorageBackend:
    """
    Initializes and returns the local storage backend.
    This is set to LocalFileStorage for testing purposes.
    """
    logger.info("Using LocalFileStorage backend for this session.")
    return LocalFileStorage(base_dir=Path("data"))

storage = get_storage_backend()

async def process_source(source: SourceConfig) -> SourceRunResult:
    """
    Processes a single source, handling all stages from crawling to storage.
    This function is designed to be resilient, catching all exceptions and
    returning a structured result object.
    """
    run_result = SourceRunResult(source_name=source.name, status="in-progress")
    logger.info(f"=== STARTING: {source.name} ===")

    try:
        # STAGE 1: Crawl & Prefilter URLs
        stage = "crawl"
        urls: list = await storage.get_urls(source.name)
        if not urls or len(urls) == 0:
            logger.info(f"[{source.name}] No URLs in storage, starting new crawl.")
            crawled_urls = await crawl_and_collect_urls(source)
            run_result.stats.urls_found = len(crawled_urls)
            logger.info(f"[{source.name}] Crawl found {run_result.stats.urls_found} potential URLs.")
            
            urls = await prefilter_urls(crawled_urls, max_concurrency=source.max_concurrency or 20)
            run_result.stats.urls_valid = len(urls)
            logger.info(f"[{source.name}] Prefiltered to {run_result.stats.urls_valid} valid URLs.")
            await storage.save_urls(source.name, urls)
        else:
            run_result.stats.urls_found = run_result.stats.urls_valid = len(urls)
            logger.info(f"[{source.name}] Loaded {len(urls)} URLs from storage.")

        if not urls:
            raise ValueError("No valid URLs found after crawl and prefilter.")

        # STAGE 2: Get or Generate Schema
        stage = "schema"
        schema = await storage.get_schema(source.name)
        if not schema or not schema.get("baseSelector"):
            logger.info(f"[{source.name}] No valid schema in storage, generating new schema.")
            schema, usage = await generate_schema(source)
            await storage.save_schema(source.name, schema)
            logger.info(f"[{source.name}] Generated new schema using {usage} tokens.")
        else:
            logger.info(f"[{source.name}] Loaded schema with baseSelector='{schema.get('baseSelector')}'")
        
        if not schema: raise ValueError("Schema could not be loaded or generated.")

        # STAGE 3: Scrape URLs and Validate Data
        stage = "scrape"
        logger.info(f"[{source.name}] Starting scrape for {len(urls)} URLs.")
        records, scrape_stats = await scrape_urls(urls, schema, source)
        run_result.stats.records_extracted = scrape_stats.records_extracted
        run_result.stats.records_validated = scrape_stats.records_validated
        run_result.stats.records_missing_required_fields = scrape_stats.records_missing_required_fields
        
        logger.info(f"[{source.name}] Extracted {scrape_stats.records_extracted} raw records.")
        logger.info(f"[{source.name}] Validated {scrape_stats.records_validated} records.")

        if not records:
            raise ValueError("Scraping yielded 0 validated records.")

        # STAGE 4: Save Data
        stage = "storage"
        await storage.save_data(source.name, records)
        logger.info(f"[{source.name}] Saved {len(records)} course records to storage.")
        
        run_result.status = "success"
        logger.info(f"=== FINISHED: {source.name} (SUCCESS) ===")

    except Exception as e:
        logger.error(f"!!! FAILED: {source.name} at stage '{stage}' !!!")
        logger.exception(e)
        error_log = ErrorLog.from_exception(e, source_name=source.name, stage=stage)
        run_result.errors.append(error_log)
        run_result.status = "failure"

    finally:
        # Use timezone-aware UTC datetime object to resolve deprecation warning
        run_result.end_time = datetime.now(timezone.utc)

    return run_result

async def main():
    """Main entry point for the scraping job."""
    # Use timezone-aware UTC datetime object
    utc_now = datetime.now(timezone.utc)
    job_id = f"job_{utc_now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    logger.info(f"Starting scraping job with ID: {job_id}")
    logger.info(f"Log file for this run is at: {log_file_path.resolve()}")
    
    job_summary = JobSummary(job_id=job_id, total_sources=len(config.sources))

    tasks = [process_source(src) for src in config.sources]
    results = await asyncio.gather(*tasks, return_exceptions=False) # Set return_exceptions=True to return errors instead of failing

    # Use timezone-aware UTC datetime object
    job_summary.end_time = datetime.now(timezone.utc)

    for res in results:
        if isinstance(res, SourceRunResult):
            job_summary.results.append(res)
            if res.status == "success":
                job_summary.succeeded += 1
            else:
                job_summary.failed += 1
    
    generate_summary_report(job_summary)

    try:
        await storage.save_job_summary(job_summary)
    except Exception as e:
        logger.critical(f"Failed to save final job summary to storage: {e}", exc_info=True)

    logger.info("Scraping job finished.")
    if job_summary.failed > 0:
        logger.warning(f"{job_summary.failed} source(s) failed. Check the summary above and scraper.log for details.")



    # for source in config.sources:
    #     item: SourceConfig = source
    #     schema = await storage.get_schema(item.name)
    #     with open(f"{item.name}_schema.json", "w") as f:
    #         f.write(json.dumps(schema, indent=2))

    # test: SourceConfig = config.sources[0]
    # schema = await generate_schema(test)
    # print(json.dumps(schema, indent=2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (ValueError, FileNotFoundError) as e:
        logger.critical(f"A critical configuration error occurred: {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred in main execution: {e}", exc_info=True)
