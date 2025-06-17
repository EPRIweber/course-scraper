# src/scraper.py
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
    JsonCssExtractionStrategy,
    LXMLWebScrapingStrategy,
)

from src.config import SourceConfig

async def scrape_urls(
    urls: List[str],
    schema: Dict[str, Any],
    source: SourceConfig
) -> List[Dict[str, Any]]:
    return await _scrape_with_schema(
        urls=urls,
        schema=schema,
        max_concurrency=source.max_concurrency
    )

async def _scrape_with_schema(
    urls: List[str],
    schema: Dict[str, Any],
    max_concurrency: int = 10,
) -> List[Dict[str, Any]]:
    """
    Apply the JSON-CSS schema to each URL in parallel using arun_many.
    Returns a flat list of all extracted course dicts, each with a "_source_url".
    """
    # 1) Setup crawler config
    browser_cfg = BrowserConfig(headless=True, verbose=False)

    extraction_strategy = JsonCssExtractionStrategy(schema)

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        scraping_strategy=LXMLWebScrapingStrategy(),
        extraction_strategy=extraction_strategy,
    )

    all_records: List[Dict[str, Any]] = []

    # 2) Fire off all URLs in parallel
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        results = await crawler.arun_many(
            urls=urls,
            config=run_cfg,
            max_concurrency=max_concurrency
        )
    
    # 3) Parse out each page's JSON payload
    for page_result in results:
        raw = page_result.extracted_content
        try:
            items = json.loads(raw)
        except json.JSONDecodeError:
            continue

        source_url = getattr(page_result, "url", None) or getattr(page_result, "request_url", None)
        for item in items:
            item["_source_url"] = source_url
            if "course_code" in item and isinstance(item["course_code"], list) and item["course_code"]:
                str_codes: list[str] = []
                raw_codes = item.get("course_code")
                for code in raw_codes:
                    if isinstance(code, dict):
                        txt = str(code.get("text", "")).strip()
                    else:
                        txt = str(code).strip()
                    if txt:
                        str_codes.append(txt)
                if str_codes:
                    unique = sorted(set(str_codes))
                    norm = "_".join(unique)
                    item["course_code"] = norm
                    log = logging.getLogger(__name__)
                    log.debug(f"Codes {raw_codes!r} → {norm!r}")
                else:
                    item.pop("course_code", None)

            all_records.append(item)

    return all_records























# # src/scraper.py
# import json
# import logging
# from typing import List, Dict, Any, Tuple

# from crawl4ai import (
#     AsyncWebCrawler,
#     BrowserConfig,
#     CrawlerRunConfig,
#     CacheMode,
#     JsonCssExtractionStrategy,
#     LXMLWebScrapingStrategy,
# )

# # Use relative imports for modules within the same package
# from .config import SourceConfig
# from .models import RunStatistics

# logger = logging.getLogger(__name__)

# async def scrape_urls(
#     urls: List[str],
#     schema: Dict[str, Any],
#     source: SourceConfig,
# ) -> Tuple[List[Dict[str, Any]], RunStatistics]:
#     """
#     Scrapes a list of URLs based on a schema and validates the results.

#     Returns:
#         A tuple containing:
#         - A list of validated records.
#         - A RunStatistics object with metrics from the scrape.
#     """
#     stats = RunStatistics()
#     if not urls:
#         return [], stats

#     all_extracted_records = await _scrape_with_schema(
#         urls=urls,
#         schema=schema,
#         max_concurrency=source.max_concurrency or 10,
#         source_name=source.name
#     )
#     stats.records_extracted = len(all_extracted_records)

#     if not all_extracted_records:
#         logger.warning(f"[{source.name}] Scrape completed but found 0 records from {len(urls)} URLs. This is a silent error.")
#         return [], stats

#     validated_records, stats.records_missing_required_fields = _validate_records(all_extracted_records, source.name)
#     stats.records_validated = len(validated_records)

#     if stats.records_missing_required_fields > 0:
#         logger.warning(
#             f"[{source.name}] Discarded {stats.records_missing_required_fields} records due to missing 'course_title' or 'course_description'."
#         )

#     return validated_records, stats

# def _validate_records(records: List[Dict[str, Any]], source_name: str) -> Tuple[List[Dict[str, Any]], int]:
#     """
#     Validates that each record has the required non-empty fields.
#     Returns a list of valid records and a count of invalid ones.
#     """
#     validated = []
#     invalid_count = 0
#     for record in records:
#         title = record.get("course_title")
#         description = record.get("course_description")
        
#         if isinstance(title, str) and title.strip() and isinstance(description, str) and description.strip():
#             validated.append(record)
#         else:
#             invalid_count += 1
#             logger.debug(f"[{source_name}] Invalid record dropped (missing title/desc): {record}")
#     return validated, invalid_count

# async def _scrape_with_schema(
#     urls: List[str],
#     schema: Dict[str, Any],
#     max_concurrency: int,
#     source_name: str,
# ) -> List[Dict[str, Any]]:
#     """
#     Apply the JSON-CSS schema to each URL in parallel.
#     Returns a flat list of all extracted course dicts.
#     """
#     browser_cfg = BrowserConfig(headless=True, verbose=False)
#     extraction_strategy = JsonCssExtractionStrategy(schema)
#     run_cfg = CrawlerRunConfig(
#         cache_mode=CacheMode.BYPASS,
#         scraping_strategy=LXMLWebScrapingStrategy(),
#         extraction_strategy=extraction_strategy,
#     )
#     all_records: List[Dict[str, Any]] = []

#     async with AsyncWebCrawler(config=browser_cfg) as crawler:
#         results = await crawler.arun_many(
#             urls=urls,
#             config=run_cfg,
#             max_concurrency=max_concurrency
#         )
    
#     for page_result in results:
#         # FIXED: Updated error checking to be compatible with new crawl4ai versions.
#         # We check for the 'error' attribute safely and also check for empty content.
#         if getattr(page_result, 'error', None):
#             logger.error(f"[{source_name}] Error scraping {page_result.url}: {page_result.error}")
#             continue

#         raw = page_result.extracted_content
#         if not raw:
#             logger.warning(f"[{source_name}] No content extracted from {page_result.url}")
#             continue

#         try:
#             items = json.loads(raw)
#             if not isinstance(items, list):
#                 logger.warning(f"[{source_name}] Expected a list from JSON, but got {type(items)} from {page_result.url}. Content: {raw[:150]}")
#                 continue
#         except (json.JSONDecodeError, TypeError):
#             logger.error(f"[{source_name}] Failed to decode JSON from {page_result.url}. Content: '{raw[:150]}...'")
#             continue

#         source_url = getattr(page_result, "url", "unknown_url")
#         for item in items:
#             if not isinstance(item, dict): continue

#             item["_source_url"] = source_url
#             if "course_code" in item and isinstance(item["course_code"], list) and item["course_code"]:
#                 str_codes = [str(c.get("text", c) if isinstance(c, dict) else c).strip() for c in item["course_code"]]
#                 unique_codes = sorted(set(filter(None, str_codes)))
#                 if unique_codes:
#                     item["course_code"] = "_".join(unique_codes)
#                 else:
#                     item.pop("course_code", None)

#             all_records.append(item)

#     return all_records
