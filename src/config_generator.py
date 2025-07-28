# src/config_generator.py
import asyncio
from collections import OrderedDict
import json
import os
import logging
from typing import List, Optional, Tuple
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.utils import get_content_of_website_optimized

from crawl4ai import AsyncWebCrawler
import httpx

from src.crawler import crawl_and_collect_urls
from src.render_utils import fetch_page

from .llm_client import GemmaModel
from .prompts.catalog_urls import CatalogRootPrompt, CatalogSchemaPrompt

from .config import SourceConfig

logger = logging.getLogger(__name__)

_GOOGLE_SEARCH_SEM = asyncio.BoundedSemaphore(1)
# _FETCH_PAGE_SEM = asyncio.BoundedSemaphore(1)


GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX = os.getenv("GOOGLE_CX")

KEYWORDS = ["catalog", "bulletin", "course", "curriculum", "description", "current"]

async def discover_source_config(name: str) -> tuple[SourceConfig, int, int]:
    """Discover a ``SourceConfig`` for ``name``."""
    root, schema, root_usage, schema_usage = await discover_catalog_urls(name)
    
    pr = urlparse(root)
    ps = urlparse(schema)

    shared_domain = f"{pr.scheme}://{pr.netloc}"
    url_base_exclude = ""

    # if they're on the same host but schema isn't a sub‑path of root
    if pr.netloc == ps.netloc:
        root_path = pr.path.rstrip("/") + "/"
        if not ps.path.startswith(root_path):
            url_base_exclude = shared_domain

    return SourceConfig(
        source_id=f"LOCAL_{name}",
        name=name,
        root_url=root,
        schema_url=schema,
        url_base_exclude=url_base_exclude
    ), root_usage, schema_usage

async def discover_catalog_urls(school: str) -> Tuple[str, str, int, int]:
    """Return root and schema URLs discovered for ``school``."""
    query = f"{school} course description catalog bulletin site"
    try:
        results = await google_search(query)
    except Exception as e:
        logger.warning("Search failed for %s", school)
        raise e
    
    
    top_hits = results[:3]

    # 1) build a flat list of {url,snippet} dicts
    pages: List[dict] = []
    for hit in top_hits:
        temp = SourceConfig(
            source_id=f"TEMP_{school}",
            name=school,
            root_url=hit,
            schema_url=hit,
            crawl_depth=1,
            url_exclude_patterns=["search", "archive"],
        )
        # crawl just one hop out from each hit
        sub_urls = await crawl_and_collect_urls(
            temp,
            make_root_filter=False,
            max_links_per_page=10
        )
        # only keep “catalog”‑y ones
        catalogs = filter_catalog_urls(sub_urls)
        # build a small list you’ll actually fetch snippets for:
        to_fetch = [hit] + catalogs[:20]   # 1 + up to 9 = 10 pages/site

        # fetch and append
        pages += await fetch_snippets(to_fetch)
    

    root_url, root_usage = await llm_select_root(school, pages) or (None, 0)
    if not root_url:
        raise Exception(f"No root URL found for {school}")
    
    pr = urlparse(root_url)
    shared_domain = f"{pr.scheme}://{pr.netloc}"

    temp = SourceConfig(
        source_id=f"TEMP_{school}",
        name=school,
        root_url=root_url,
        schema_url=root_url,
        url_base_exclude=shared_domain,
        crawl_depth=3
    )
    all_urls = await crawl_and_collect_urls(
        temp,
        # make_root_filter=False,
        # max_links_per_page=50
    )
    seen = set(); unique = []
    for u in all_urls:
        if u not in seen:
            seen.add(u); unique.append(u)

    schema_pages = await fetch_snippets(unique[:min(100, len(unique))])
    schema_url, schema_usage = await llm_select_schema(school, root_url, schema_pages) or (None, 0)
    if not schema_url:
        raise Exception(f"No schema URL returned for {school}")
    return root_url, schema_url, root_usage, schema_usage

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

async def fetch_snippets(urls: List[str]) -> List[dict]:
    """Fetch each URL via Playwright+HTTPX fallback and hand back raw HTML."""
    pages = []
    for url in urls:
        try:
            html = await fetch_page(url)
            markdown_page = get_content_of_website_optimized(url, html)
            pages.append({"url": url, "snippet": markdown_page})
            await asyncio.sleep(0.2)
        except Exception as e:
            logger.debug("Failed to fetch %s for snippet: %s", url, e)
    return pages

async def google_search(query: str, *, count: int = 4) -> List[str]:
    """Return a list of result URLs from Google Programmable Search."""
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        raise RuntimeError(
            "GOOGLE_API_KEY and GOOGLE_CX environment variables are required"
        )
    
    query = query.replace("TESTING", "")

    async with _GOOGLE_SEARCH_SEM:
        params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": query, "num": count}
        async with httpx.AsyncClient(timeout=60000 * 10, verify=False) as client:
            resp = await client.get(GOOGLE_CSE_ENDPOINT, params=params)
            resp.raise_for_status()
            data = resp.json()
        return [item["link"] for item in data.get("items", [])]

def filter_catalog_urls(urls: List[str]) -> List[str]:
    filtered = []
    for url in urls:
        lower = url.lower()
        if any(k in lower for k in KEYWORDS) and ".edu" in lower and ('pdf' not in lower):
            filtered.append(url)
    return filtered

async def llm_select_root(school: str, pages: List[dict]) -> tuple[str, int]:
    """Use the LLM to choose the best root URL from pre-fetched ``pages``."""
    # print("⟳ pages passed into llm_select_root:", pages)
    logger.debug("REACHED llm_select_root")
    if not pages:
        logger.warning("No pages provided to llm_select_root")
        raise Exception(f"No pages provided to llm_select_root for {school}")
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
                {"role": "user", "content": user_p[:min(250_000, len(user_p))]},
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
                raise ex
        else:
            logger.warning("Root LLM returned non-list data: %s", data)
            raise Exception("Root LLM returned non-list data: %s", data)
        # print(resp)
        prompt_t = resp.get("usage", {}).get("prompt_tokens")
        completion_t = resp.get("usage", {}).get("completion_tokens")

        return url, prompt_t + completion_t
    except Exception as e:
        logger.warning("LLM root selection failed for %s", school)
        raise e

async def llm_select_schema(
    school: str,
    root_url: str,
    pages: List[str]
) -> tuple[str, int]:
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
                {"role": "user", "content": user_p[:min(250_000, len(user_p))]},
            ]
        )
        data = json.loads(resp["choices"][0]["message"]["content"])
        if isinstance(data, dict):
            try:
                print(data)
                url = data.get("schema_url")
            except Exception as ex:
                logger.warning(f"Failed to load URL from LLM response, instead received: {data}")
                raise ex
        else:
            logger.warning("Schema LLM returned non-list data: %s", data)
            raise Exception("Schema LLM returned non-list data: %s", data)
        # print(resp)
        prompt_t = resp.get("usage", {}).get("prompt_tokens")
        completion_t = resp.get("usage", {}).get("completion_tokens")

        return url, prompt_t + completion_t
    except Exception as e:
        logger.warning("LLM schema selection failed for %s", school)
        raise e
