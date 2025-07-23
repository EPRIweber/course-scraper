# src/config_generator.py
from collections import OrderedDict
import json
import os
import logging
from typing import List, Optional, Tuple
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

from crawl4ai import AsyncWebCrawler
import httpx

from src.crawler import crawl_and_collect_urls

from .llm_client import GemmaModel
from .prompts.catalog_urls import CatalogRootPrompt, CatalogSchemaPrompt

from .config import SourceConfig

logger = logging.getLogger(__name__)

GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX = os.getenv("GOOGLE_CX")

KEYWORDS = ["catalog", "bulletin", "courses", "curriculum"]

async def discover_source_config(name: str) -> SourceConfig:
    """Discover a ``SourceConfig`` for ``name``."""
    root, schema = await discover_catalog_urls(name)
    return SourceConfig(
        source_id=f"LOCAL_{name}",
        name=name,
        root_url=root,
        schema_url=schema,
    )


async def discover_catalog_urls(school: str) -> Optional[Tuple[str, str]]:
    """Return root and schema URLs discovered for ``school``."""
    query = f"{school} course description catalog bulletin site"
    try:
        results = await google_search(query)
    except Exception as e:
        logger.warning("Search failed for %s", school)
        raise logger.exception(e)

    candidates = filter_catalog_urls(results)
    combined = candidates + results
    ordered_by_priority = list(OrderedDict.fromkeys(combined))

    browser_cfg = BrowserConfig(headless=True, verbose=False)
    run_cfg = make_markdown_run_cfg(timeout_s=30)

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        pages = await fetch_snippets(crawler, ordered_by_priority[:5], run_cfg)
        root_res = await llm_select_root(school, pages)
        if not root_res:
            return None
        root_url, _ = root_res

        temp = SourceConfig(
            source_id=f"TEMP_{school}",
            name=school,
            root_url=root_url,
            schema_url=root_url,
        )
        all_urls = await crawl_and_collect_urls(temp)
        schema_pages = await fetch_snippets(crawler, all_urls[:5], run_cfg)
        schema_res = await llm_select_schema(school, root_url, schema_pages)
        if not schema_res:
            return None
        schema_url, _ = schema_res
        return root_url, schema_url

def make_markdown_run_cfg(timeout_s: int) -> CrawlerRunConfig:
    """Return a crawler run configuration for Markdown extraction."""
    return CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        markdown_generator=DefaultMarkdownGenerator(
            content_filter=PruningContentFilter(threshold=0.5),
            options={"ignore_links": True},
        ),
        page_timeout=timeout_s * 1000,
    )

async def fetch_snippets(
    crawler: AsyncWebCrawler, urls: List[str], run_cfg: CrawlerRunConfig, *, max_concurrency: int = 1
) -> list[dict]:
    """Fetch pages with the crawler and return Markdown snippets."""
    results = await crawler.arun_many(urls, config=run_cfg, max_concurrency=max_concurrency)
    pages = []
    for r in results:
        if getattr(r, "success", False):
            snippet = getattr(getattr(r, "markdown", None), "fit_markdown", None)
            pages.append({"url": r.url, "snippet": snippet})
    return pages

async def google_search(query: str, *, count: int = 5) -> List[str]:
    """Return a list of result URLs from Google Programmable Search."""
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        raise RuntimeError(
            "GOOGLE_API_KEY and GOOGLE_CX environment variables are required"
        )
    params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": query, "num": count}
    async with httpx.AsyncClient(timeout=60000 * 10) as client:
        resp = await client.get(GOOGLE_CSE_ENDPOINT, params=params)
        resp.raise_for_status()
        data = resp.json()
    return [item["link"] for item in data.get("items", [])]

def filter_catalog_urls(urls: List[str]) -> List[str]:
    filtered = []
    for url in urls:
        lower = url.lower()
        if any(k in lower for k in KEYWORDS) and ".edu" in lower:
            filtered.append(url)
    return filtered

async def llm_select_root(school: str, pages: List[dict]) -> Optional[tuple[str, int]]:
    """Use the LLM to choose the best root URL from pre-fetched ``pages``."""
    # print("⟳ pages passed into llm_select_root:", pages)
    logger.debug("REACHED llm_select_root")
    if not pages:
        logger.warning("No pages provided to llm_select_root")
        return None
    prompt = CatalogRootPrompt(school, pages)
    llm = GemmaModel()
    llm.set_response_format({
        "type": "json_object",
        "json_schema": {
            "name": "CourseExtractionSchema",
            "description": "CourseExtractionSchema",
            "root_url": {
                "type": "string"
            },
            "strict": True
        }
    })
    try:
        sys_p = prompt.system()
        user_p = prompt.user()
        resp = llm.chat(
            [
                {"role": "system", "content": sys_p},
                {"role": "user", "content": user_p},
            ]
        )
        # print(f"SYSTEM PROMPT:\n{sys_p}\n\n\u25B6 USER PROMPT:\n{user_p}\n")
        data = json.loads(resp["choices"][0]["message"]["content"])
        if isinstance(data, dict):
            try:
                # print(data)
                url = data.get("root_url")
            except Exception as ex:
                logger.warning(f"Failed to load URL from LLM response, instead received: {data}")
                return None
        else:
            logger.warning("Root LLM returned non-list data: %s", data)
            return None
        # print(resp)
        prompt_t = resp.get("usage", {}).get("prompt_tokens")
        completion_t = resp.get("usage", {}).get("completion_tokens")

        return url, prompt_t + completion_t
    except Exception as e:
        logger.warning("LLM root selection failed for %s", school)
        return None

async def llm_select_schema(
    school: str,
    root_url: str,
    pages: List[str]
) -> Optional[tuple[str, int]]:
    prompt = CatalogSchemaPrompt(school, root_url, pages)
    sys_p = prompt.system()
    user_p = prompt.user()
    # print(f"SYSTEM PROMPT:\n{sys_p}\n\n\u25B6 USER PROMPT:\n{user_p}\n")
    llm = GemmaModel()
    llm.set_response_format({
        "type": "json_object",
        "json_schema": {
            "name": "CourseExtractionSchema",
            "description": "CourseExtractionSchema",
            "schema_url": {
                "type": "string"
            },
            "strict": True
        }
    })
    try:
        resp = llm.chat(
            [
                {"role": "system", "content": sys_p},
                {"role": "user", "content": user_p},
            ]
        )
        data = json.loads(resp["choices"][0]["message"]["content"])
        if isinstance(data, dict):
            try:
                print(data)
                url = data.get("schema_url")
            except Exception as ex:
                logger.warning(f"Failed to load URL from LLM response, instead received: {data}")
                return None
        else:
            logger.warning("Schema LLM returned non-list data: %s", data)
            return 
        # print(resp)
        prompt_t = resp.get("usage", {}).get("prompt_tokens")
        completion_t = resp.get("usage", {}).get("completion_tokens")

        return url, prompt_t + completion_t
    except Exception as e:
        logger.warning("LLM schema selection failed for %s", school)
        return None


# async def generate_config(name: str, ipeds_url: Optional[str] = None) -> List[SourceConfig]:
#     source: SourceConfig = None
#     print(f"Discovering catalog for {name}...")
#     try:
#         res = await discover_catalog_urls(name)
#     except Exception as e:
#         logger.warning("Failed to discover catalog URLs for %s", name)
#         return None
#     if not res:
#         logger.warning("No catalog found %s", name)
#         return None
#     root_url, schema_url = res
#     src = create_source(name, root_url, schema_url)
#     print(f"  found: {root_url} -> {schema_url}")
#     return src

# def update_sources_file(new_sources: List[SourceConfig]) -> None:
#     data = {}
#     existing = data.setdefault("sources", [])
#     for src in new_sources:
#         existing.append(
#             src.model_dump(mode="json", exclude_defaults=True, exclude_none=True)
#         )

#     data["sources"] = existing
#     print(yaml.safe_dump(data, sort_keys=False))
#     with open("configs/test_source_generation.yaml", "w") as f:
#         yaml.safe_dump(data, f, sort_keys=False)

# def load_names_from_csv(csv_path: Path) -> List[str]:
#     with open(csv_path) as f:
#         return [row[0] for row in csv.reader(f) if row]

# async def async_main() -> None:
#     names = []
#     with open("configs/new_schools.csv", "r", newline="") as file:
#         csv_reader = csv.reader(file)
#         for row in csv_reader:
#             names.append(row[0])
#     try:
#         sources = await generate_for_schools(names)
#         if not sources:
#             logger.warning("No sources generated")
#             return
#         update_sources_file(sources)
#     finally:
#         await close_playwright()


# FOR TESTING PURPOSES

# def main() -> None:
#     asyncio.run(async_main())

# if __name__ == "__main__":
#     main()
