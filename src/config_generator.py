# src/config_generator.py
import argparse
import asyncio
from collections import OrderedDict
import csv
import json
import os
import logging
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin

from crawl4ai import AsyncWebCrawler
import httpx
from bs4 import BeautifulSoup
from crawl4ai.utils import get_content_of_website_optimized

from src.crawler import crawl_and_collect_urls

from .render_utils import close_playwright, fetch_page
import yaml
from .llm_client import LlamaModel, GemmaModel
from .prompts.catalog_urls import CatalogRootPrompt, CatalogSchemaPrompt

from .config import SourceConfig

logger = logging.getLogger(__name__)

GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX = os.getenv("GOOGLE_CX")

KEYWORDS = ["catalog", "bulletin", "courses", "curriculum"]

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

async def get_markdown_snippet(
    url: str,
    limit: int = 60000,
    html: Optional[str] = None,
) -> Optional[str]:
    """Return a cleaned markdown snippet for ``url``."""
    try:
        if html is None:
            html = await fetch_page(url)
        data = get_content_of_website_optimized(url, html)
        snippet = data.get("markdown", "")
        return snippet
    except Exception as e:
        logger.warning("Failed to get markdown snippet for %s", url)
        raise logger.exception(e)

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
                print(data)
                url = data.get("root_url")
            except Exception as ex:
                logger.warning(f"Failed to load URL from LLM response, instead received: {data}")
                raise logger.exception(ex)
        else:
            logger.warning("Root LLM returned non-list data: %s", data)
            return None
        print(resp)
        prompt_t = resp.get("usage", {}).get("prompt_tokens")
        completion_t = resp.get("usage", {}).get("completion_tokens")

        return url, prompt_t + completion_t
    except Exception as e:
        logger.warning("LLM root selection failed for %s", school)
        raise logger.exception(e)

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
                raise logger.exception(ex)
        else:
            logger.warning("Schema LLM returned non-list data: %s", data)
            return 
        # print(resp)
        prompt_t = resp.get("usage", {}).get("prompt_tokens")
        completion_t = resp.get("usage", {}).get("completion_tokens")

        return url, prompt_t + completion_t
    except Exception as e:
        logger.warning("LLM schema selection failed for %s", school)
        raise logger.exception(e)

async def discover_catalog_urls(school: str) -> Optional[Tuple[str, str]]:
    query = f"{school} course description catalog bulletin site"
    try:
        results = await google_search(query)
    except Exception as e:
        logger.warning("Search failed for %s", school)
        raise logger.exception(e)
    candidates = filter_catalog_urls(results)
    combined = candidates + results
    ordered_by_priority = list(OrderedDict.fromkeys(combined))

    sem = asyncio.Semaphore(5)
    pages: list[dict] = []

    async def process(url: str) -> None:
        async with sem:
            try:
                html = await fetch_page(url)
                snippet = await get_markdown_snippet(url, html=html)
                if snippet:
                    pages.append({"url": url, "snippet": snippet})
            except Exception as e:
                logger.warning("Failed processing %s", url)
                raise logger.exception(e)

    await asyncio.gather(*(process(u) for u in ordered_by_priority))

    # for p in pages:
    #     print(p["url"], "snippet_len=", len(p["snippet"]))

    root_url, usage = await llm_select_root(school, pages)

    if not root_url:
        logger.warning("LLM did not select a root URL, returning None")
        return None

    temp_source = SourceConfig(
       source_id=f"TEMP_{school}",
       name=school,
       root_url=root_url,
       schema_url=root_url
    )
    links = await crawl_and_collect_urls(temp_source)
    print(links)
    # Implement crawl.arun_many using get_content_of_website_optimized in markdown strategy here.
    # If not possible to use get_content_of_website_optimized with arun_amany, then use the next best markdown generator strategy.

    # Pass in scraped pages into llm_select_schema from here:
    schema_url, usage = await llm_select_schema(
        school=school,
        root_url=root_url,
        pages=[]
    )
    if schema_url:
        return root_url, schema_url
    else:
        logger.warning("LLM did not select a schema URL, returning None")
        return None


def create_source(name: str, root_url: str, schema_url: str) -> SourceConfig:
    return SourceConfig(
        source_id=f"LOCAL_{name}",
        name=name,
        root_url=root_url,
        schema_url=schema_url,
    )

async def generate_for_schools(names: List[str]) -> List[SourceConfig]:
    sources: List[SourceConfig] = []
    for name in names:
        print(f"Discovering catalog for {name}...")
        try:
            res = await discover_catalog_urls(name)
        except Exception as e:
            logger.warning("Failed to discover catalog URLs for %s", name)
            raise logger.exception(e)
        if not res:
            print(f"  no catalog found")
            continue
        root_url, schema_url = res
        src = create_source(name, root_url, schema_url)
        sources.append(src)
        print(f"  found: {root_url} -> {schema_url}")
    return sources

def update_sources_file(new_sources: List[SourceConfig]) -> None:
    data = {}
    existing = data.setdefault("sources", [])
    for src in new_sources:
        existing.append(
            src.model_dump(mode="json", exclude_defaults=True, exclude_none=True)
        )

    data["sources"] = existing
    print(yaml.safe_dump(data, sort_keys=False))
    with open("configs/test_source_generation.yaml", "w") as f:
        yaml.safe_dump(data, f, sort_keys=False)

def load_names_from_csv(csv_path: Path) -> List[str]:
    with open(csv_path) as f:
        return [row[0] for row in csv.reader(f) if row]

async def async_main() -> None:
    names = []
    with open("configs/new_schools.csv", "r", newline="") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            names.append(row[0])
    try:
        sources = await generate_for_schools(names)
        if not sources:
            logger.warning("No sources generated")
            return
        update_sources_file(sources)
    finally:
        await close_playwright()


# FOR TESTING PURPOSES

def main() -> None:
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
