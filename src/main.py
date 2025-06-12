# src/main.py
import asyncio
import logging
import os
from pathlib import Path

from src.config import config      # your AppConfig instance
from src.crawler import crawl_and_collect_urls
from src.prefilter import prefilter_urls
from src.schema_manager import generate_schema
from src.scraper import scrape_with_schema
from src.storage import LocalFileStorage, FirestoreStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Choose storage backend via env-var: 'local' or 'firestore' (default)
backend_name = os.getenv("STORAGE_BACKEND", "firestore").lower()
if backend_name == "local":
    storage = LocalFileStorage(base_dir=Path("data"))
else:
    storage = FirestoreStorage(project=os.getenv("GCP_PROJECT"))

async def process_source(source):
    name = source.name
    logger.info(f"=== STARTING: {name} ===")

    # 1) Crawl & collect URLs
    urls = storage.get_urls(name)
    if not urls or len(urls) == 0:
        logger.info(f"[{name}] No schema in storage, generating new one")
        urls = await crawl_and_collect_urls(source)
        logger.info(f"[{name}] Collected {len(urls)} URLs")
        urls = await prefilter_urls(urls, max_concurrency=20, timeout=2.0)
        logger.info(f"[{name}] Prefiltered to {len(urls)} valid URLs")
        storage.save_urls(name, urls)
    else:
        logger.info(f"[{name}] Loaded {len(urls)} URLs from storage")
    if not urls:
        logger.warning(f"[{name}] No valid URLs found, skipping source")
        return

    # 2) Get or generate schema
    schema = storage.get_schema(name)
    if not schema or not schema.get("baseSelector"):
        logger.info(f"[{name}] No cached schema found, generating new one")
        schema = generate_schema(source)
        storage.save_schema(name, schema)
        logger.info(f"[{ name}] Saved new schema")
    else:
        logger.info(f"[{name}] Loaded cached schema with baseSelector={schema['baseSelector']}")

    # 3) Scrape each URL
    records = storage.get_data(name)
    if not records or len(records) == 0:
        logger.info(f"[{name}] No cached records found, starting scrape")
        records = await scrape_with_schema(urls, schema, source)
        storage.save_data(name, records)
        logger.info(f"[{name}] Extracted {len(records)} course records")
    else:
        logger.info(f"[{name}] Loaded {len(records)} cached course records")

async def main():
    tasks = [process_source(src) for src in config.sources]
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
