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
) -> tuple[List[Dict[str, Any]], set[str], set[str], list[Any]]:
    records, good_urls, json_errors  = await _scrape_with_schema(
        urls=urls,
        schema=schema,
        max_concurrency=source.max_concurrency if source.max_concurrency is not None else 5
    )
    bad_urls       = set(urls) - good_urls
    return records, good_urls, bad_urls, json_errors

async def _scrape_with_schema(
    urls: List[str],
    schema: Dict[str, Any],
    max_concurrency: int,
) -> tuple[List[Dict[str, Any]], set[str], list[Any]]:
    """
    Apply the JSON-CSS schema to each URL in parallel using arun_many.
    Returns a flat list of all extracted course dicts, each with a "_source_url".
    """
    log = logging.getLogger(__name__)
    # 1) Setup crawler config
    browser_cfg = BrowserConfig(headless=True, verbose=False)

    extraction_strategy = JsonCssExtractionStrategy(schema)

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        scraping_strategy=LXMLWebScrapingStrategy(),
        extraction_strategy=extraction_strategy,
    )

    all_records: List[Dict[str, Any]] = []
    good_pages: set[str] = set()
    json_errors = []

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

        if not raw:
            log.error(f"No extracted content from {page_result.url}")
            json_errors.append(f"No extracted content from {page_result.url}")
            continue


        try:
            items = json.loads(raw)
        except json.JSONDecodeError:
            log.error(f"Failed to decode JSON from {page_result.url}: {raw[:100]}...")
            json_errors.append(f"JSON errors on {page_result.url}: {raw[:100]}{"..." if len(raw) > 100 else ""}")
            continue

        source_url = getattr(page_result, "url", None) or getattr(page_result, "request_url", None)

        if items:
            good_pages.add(source_url)
        
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
                    log.debug(f"Codes {raw_codes!r} → {norm!r}")
                else:
                    item.pop("course_code", None)

            all_records.append(item)

    return all_records, good_pages, json_errors
