# src/main.py
import asyncio
import logging
from pathlib import Path

from src.config import config      # your AppConfig instance
from src.crawler import crawl_and_collect_urls
from src.prefilter import prefilter_urls
from src.schema_manager import get_or_generate
from src.scraper import scrape_with_schema
from src.storage import LocalFileStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

async def process_source(source):
    name = source.name
    logger.info(f"=== STARTING: {name} ===")
    storage = LocalFileStorage(base_dir=Path("data"))

    # 1) Crawl & collect URLs
    urls = await crawl_and_collect_urls(source)
    storage.save_urls(name, urls)
    logger.info(f"[{name}] Collected {len(urls)} URLs")

    # 2) Get or generate schema
    schema = get_or_generate(source)
    logger.info(f"[{name}] Loaded schema with baseSelector={schema['baseSelector']}")

    # 3) Prefilter out 404s
    good_urls = await prefilter_urls(urls, max_concurrency=50, timeout=1.0)
    logger.info(f"[{source.name}] {len(good_urls)}/{len(urls)} URLs passed HTTP check")

    # 4) Scrape each URL
    records = await scrape_with_schema(good_urls, schema)
    storage.save_data(name, records)
    logger.info(f"[{name}] Extracted {len(records)} course records")

async def main():
    tasks = [process_source(src) for src in config.sources]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
