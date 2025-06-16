# src/main.py
import asyncio
import json
import logging
import os
from pathlib import Path
from .prompts.find_repeating import FindRepeating

from src.config import SourceConfig, config
from src.crawler import crawl_and_collect_urls
from src.prefilter import prefilter_urls
from src.schema_manager import generate_schema
from src.scraper import scrape_urls
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

async def process_source(source: SourceConfig):
    name = source.name
    logger.info(f"=== STARTING: {name} ===")

    # 1) Crawl & collect URLs
    urls = await storage.get_urls(name)
    if not urls or len(urls) == 0:
        logger.info(f"[{name}] No URLs in storage, starting crawl")
        urls = await crawl_and_collect_urls(source)
        logger.info(f"[{name}] Collected {len(urls)} URLs")
        urls = await prefilter_urls(urls, source)
        logger.info(f"[{name}] Prefiltered to {len(urls)} valid URLs")
        await storage.save_urls(name, urls)
    else:
        logger.info(f"[{name}] Loaded {len(urls)} URLs from storage")
    if not urls:
        logger.warning(f"[{name}] No valid URLs found, skipping source")
        return

    # 2) Get or generate schema
    schema = await storage.get_schema(name)
    if not schema or not schema.get("baseSelector"):
        logger.info(f"[{name}] No schema in storage, generating new one")
        schema = await generate_schema(source)
        await storage.save_schema(name, schema)
        logger.info(f"[{ name}] Saved new schema")
    else:
        logger.info(f"[{name}] Loaded schema with baseSelector={schema['baseSelector']}")

    # 3) Scrape each URL
    records = await storage.get_data(name)
    if not records or len(records) == 0:
        logger.info(f"[{name}] No course data in storage, starting scrape")
        records = await scrape_urls(urls, schema, source)
        await storage.save_data(name, records)
        logger.info(f"[{name}] Extracted {len(records)} course records")
    else:
        logger.info(f"[{name}] Loaded {len(records)} records from storage")

async def main():
    # tasks = [process_source(src) for src in config.sources]
    # await asyncio.gather(*tasks, return_exceptions=False)  # Set return_exceptions=True to return errors instead of failing



    # for source in config.sources:
    #     item: SourceConfig = source
    #     schema = await storage.get_schema(item.name)
    #     with open(f"{item.name}_schema.json", "w") as f:
    #         f.write(json.dumps(schema, indent=2))

    test : SourceConfig = config.sources[0]
    schema = await generate_schema(test)
    print(schema)


if __name__ == "__main__":
    asyncio.run(main())

