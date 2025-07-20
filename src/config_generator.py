# src/config_generator.py
import argparse
import asyncio
from collections import OrderedDict
import csv
import json
import os
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.crawler import crawl_and_collect_urls

from .render_utils import close_playwright
import yaml
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from .llm_client import LlamaModel, GemmaModel
from .prompts.catalog_urls import CatalogRootPrompt, CatalogSchemaPrompt

from .config import SourceConfig

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


async def fetch_html(url: str, crawler: AsyncWebCrawler) -> Optional[str]:
    """Fetch ``url`` using ``crawler`` and return the raw HTML."""
    try:
        result = await crawler.arun(url=url, config=CrawlerRunConfig(cache_mode=CacheMode.ENABLED))
        return result.html if result.success else None
    except Exception:
        return None


async def get_markdown_snippet(
    url: str,
    limit: int = 60000,
    crawler: Optional[AsyncWebCrawler] = None,
) -> Optional[str]:
    """Return a cleaned markdown snippet for ``url`` using Crawl4AI."""
    if crawler is None:
        async with AsyncWebCrawler(config=BrowserConfig(headless=True, verbose=False)) as _crawler:
            return await get_markdown_snippet(url, limit, _crawler)
    try:
        result = await crawler.arun(
            url=url,
            config=CrawlerRunConfig(
                cache_mode=CacheMode.ENABLED,
                markdown_generator=DefaultMarkdownGenerator(
                    content_filter=PruningContentFilter(threshold=0.5),
                    options={"ignore_links": True},
                ),
            ),
        )
        if not result.success:
            return None
        snippet = result.markdown.fit_markdown or ""
        return snippet[:limit]
    except Exception:
        return None


def find_course_link(html: str, base_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # lower = href.lower()
        # if any(k in lower for k in ["preview_course", "courses", "coursedog"]):
        return urljoin(base_url, href)
    return None


async def llm_select_root(school: str, pages: List[dict]) -> Optional[tuple[str, int]]:
    """Use the LLM to choose the best root URL from pre-fetched ``pages``."""
    print("REACHED llm_select_root")
    if not pages:
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
        resp = await llm.chat(
            [
                {"role": "system", "content": prompt.system()},
                {"role": "user", "content": prompt.user()},
            ]
        )
        sys_p = prompt.system()
        user_p = prompt.user()
        print(f'▶ SYSTEM PROMPT:\n{sys_p}\n\n▶ USER PROMPT:\n{user_p}\n')
        data = json.loads(resp["choices"][0]["message"]["content"])
        if isinstance(data, list):
            try:
                url = data.get("root_url")
            except Exception as ex:
                raise RuntimeError(f"Failed to load URL from LLM response, instead received: {data}") from ex

        usage = resp.get("usage", {})

        return url, usage
    except Exception:
        return None


async def llm_select_schema(
    school: str,
    root_url: str,
    candidates: List[str],
    crawler: Optional[AsyncWebCrawler] = None,
) -> Optional[tuple[str, int]]:
    pages = []
    for url in candidates[:3]:
        snippet = await get_markdown_snippet(url, crawler=crawler)
        if snippet:
            pages.append({"url": url, "snippet": snippet})
    if not pages:
        return None
    prompt = CatalogSchemaPrompt(school, root_url, pages)
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
        resp = await llm.chat(
            [
                {"role": "system", "content": prompt.system()},
                {"role": "user", "content": prompt.user()},
            ]
        )
        sys_p = prompt.system()
        user_p = prompt.user()
        print(f'▶ SYSTEM PROMPT:\n{sys_p}\n\n▶ USER PROMPT:\n{user_p}\n')
        data = json.loads(resp["choices"][0]["message"]["content"])
        if isinstance(data, list):
            try:
                url = data.get("schema_url")
            except Exception as ex:
                raise RuntimeError(f"Failed to load URL from LLM response, instead received: {data}") from ex
        usage = resp.get("usage", {})

        return url, usage
    except Exception:
        return None


async def analyze_candidate(url: str, crawler: AsyncWebCrawler) -> Optional[Tuple[str, str]]:
    html = await fetch_html(url, crawler)
    if html is None:
        return None
    course_url = find_course_link(html, url)
    if course_url:
        return url, course_url
    return None


async def discover_catalog_urls(school: str) -> Optional[Tuple[str, str]]:
    query = f"{school} course description catalog bulletin site"
    try:
        results = await google_search(query)
    except Exception as e:
        print(f"Search failed for {school}: {e}")
        return None
    candidates = filter_catalog_urls(results)
    combined = candidates + results
    ordered_by_priority = list(OrderedDict.fromkeys(combined))

    browser_cfg = BrowserConfig(headless=True, verbose=False)
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        urls_to_snip = ordered_by_priority
        results_snip = await crawler.arun_many(
            urls=urls_to_snip,
            config=CrawlerRunConfig(
                cache_mode=CacheMode.ENABLED,
                markdown_generator=DefaultMarkdownGenerator(
                    content_filter=PruningContentFilter(threshold=0.5),
                    options={"ignore_links": True},
                ),
            ),
        )
        pages = [
            {"url": res.url, "snippet": res.markdown.fit_markdown}
            for res in results_snip
            if res.success and res.markdown.fit_markdown
        ]

        root_choice = await llm_select_root(school, pages)
        if root_choice:
            root_url, usage = root_choice
        else:
            root_url = None

        if not root_url:
            for url in ordered_by_priority:
                result = await analyze_candidate(url, crawler)
                if result:
                    root_url, schema_url = result
                    return root_url, schema_url
            return None

        temp_source = SourceConfig(
           source_id=f"TEMP_{school}",
           name=school,
           root_url=root_url,
           schema_url=root_url
        )
        links = await crawl_and_collect_urls(temp_source)
        schema_res = await llm_select_schema(school, root_url, links, crawler=crawler)
        if schema_res:
            schema_url, usage = schema_res
            return root_url, schema_url

        for url in ordered_by_priority:
            result = await analyze_candidate(url, crawler)
            if result:
                return result
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
        res = await discover_catalog_urls(name)
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
            print("No sources generated")
            return
        update_sources_file(sources)
    finally:
        await close_playwright()


def main() -> None:
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
