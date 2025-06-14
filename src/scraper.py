# src/scraper.py
import json
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

async def scrape_with_schema(
    urls: List[str],
    schema: Dict[str, Any],
    max_concurrency: int = 5
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
    for page_result in results: # type: ignore
        raw = page_result.extracted_content
        try:
            items = json.loads(raw)
        except json.JSONDecodeError:
            continue

        source_url = getattr(page_result, "url", None) or getattr(page_result, "request_url", None)
        for item in items:
            item["_source_url"] = source_url
            all_records.append(item)

    return all_records