# src/scraper.py
"""HTML page scraping helpers.

Given a list of URLs and a JSON/CSS schema this module uses Crawl4AI to extract
structured course information. The helpers return parsed records along with sets
of good and bad URLs for further processing.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any
import re, html, unicodedata

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
    records, good_urls, result_errors  = await _scrape_with_schema(
        urls=urls,
        schema=schema,
        max_concurrency=source.max_concurrency
    )
    bad_urls       = set(urls) - good_urls
    return records, good_urls, bad_urls, result_errors

async def _scrape_with_schema(
    urls: List[str],
    schema: Dict[str, Any],
    max_concurrency: int,
) -> tuple[List[Dict[str, Any]], set[str], list[Any]]:
    """
    Apply the JSON-CSS schema to each URL in parallel using arun_many.
    Returns a flat list of all extracted course dicts, each with a "_source_url".
    """
    def clean_text(s: str) -> str:
        # unescape any html entities
        s = html.unescape(s)
        # normalize unicode (e.g. turn “\u00a0” into actual NBSP)
        s = unicodedata.normalize("NFKC", s)
        # replace non-breaking spaces and bullet chars
        s = s.replace("\u00a0", " ").replace("\u2022", " ")
        s = re.sub(r"\n", " ", s)
        # collapse whitespace
        s = re.sub(r"\s+", " ", s)
        s = re.sub("Help (opens a new window)", "", s)
        s = re.sub("Page (opens a new window)", "", s)
        s = re.sub("Print (opens a new window)", "", s)
        s = s.replace("(opens a new window)", "")
        s = s.replace("Add to My Favorites Share this PageFacebook this Page Tweet this Page Print Help", "")
        s = re.sub(r"\d\d\d\d-\d\d\d\d ((Undergraduate)|(Graduate)) CatalogAdd to Portfolio", "", s)
        # strip leading/trailing
        return s.strip()

    log = logging.getLogger(__name__)
    # 1) Setup crawler config
    browser_cfg = BrowserConfig(headless=True, verbose=False)

    extraction_strategy = JsonCssExtractionStrategy(schema)

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        scraping_strategy=LXMLWebScrapingStrategy(),
        extraction_strategy=extraction_strategy,
        page_timeout=60000*10
    )

    all_records: List[Dict[str, Any]] = []
    good_pages: set[str] = set()
    result_errors = []

    current_urls = filter(lambda url: "archive" not in url, urls)

    # 2) Fire off all URLs in parallel
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        results = await crawler.arun_many(
            urls=current_urls,
            config=run_cfg,
            max_concurrency=max_concurrency
        )

    failures = []
    
    # 3) Parse out each page's JSON payload
    for page_result in results:
        raw = page_result.extracted_content

        if getattr(page_result, "error", None):
            failures.append((page_result.url, page_result.error))

        if not raw:
            log.error(f"No extracted content from {page_result.url}")
            result_errors.append(f"No extracted content from {page_result.url}")
            continue


        try:
            items = json.loads(raw)
        except json.JSONDecodeError:
            log.error(f"Failed to decode JSON from {page_result.url}: {raw[:100]}...")
            result_errors.append(f"JSON errors on {page_result.url}: {raw[:100]}{"..." if len(raw) > 100 else ""}")
            continue

        # --- CLEANUP: strip out unicode escapes & bullets ---
        # apply to every string field in every record
        cleaned_items = []
        for obj in items:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, str):
                        obj[k] = clean_text(v)
                    elif isinstance(v, dict):
                        for key, val in v.items():
                            if isinstance(val, str):
                                obj[key] = clean_text(val)
            cleaned_items.append(obj)
        items = cleaned_items
        # ---------------------------------------------------

        source_url = getattr(page_result, "url", None) or getattr(page_result, "request_url", None)

        if items:
            good_pages.add(source_url)
        
        for item in items:
            if ("course_title" in item) and ("course_description" in item):
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
            else:
                result_errors.append(f"Page missing course_title and/or course_description: {json.dumps(item) if isinstance(item, dict) else item}")

    return all_records, good_pages, result_errors
