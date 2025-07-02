# src/crawler.py
import asyncio
from collections import deque
import re
from typing import Set
from urllib.parse import urljoin, urlparse
import logging

import httpx
from bs4 import BeautifulSoup

from crawl4ai import AsyncWebCrawler
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy

from .config import SourceConfig

logger = logging.getLogger(__name__)

# ———————————————————————————————————————————————————————————————
# public entrypoint
# ———————————————————————————————————————————————————————————————

async def crawl_and_collect_urls(source: SourceConfig) -> list[str]:
    urls = await _static_bfs_crawl(
        root_url=str(source.root_url),
        max_crawl_depth=source.crawl_depth,
        include_external_links=source.include_external,
        concurrency=source.max_concurrency
    )
    return sorted(urls)

# ———————————————————————————————————————————————————————————————
# internal BFS crawl
# ———————————————————————————————————————————————————————————————

async def _static_bfs_crawl(
    root_url: str,
    max_crawl_depth: int,
    include_external_links: bool,
    concurrency: int
) -> Set[str]:
    start = urlparse(root_url)
    domain = start.netloc
    root_path = (start.path.rstrip("/") + "/") if start.path else "/"

    def _inside_start_path(u: str) -> bool:
        p = urlparse(u)
        return p.netloc == domain and p.path.startswith(root_path)

    class ExcludePatternFilter:
        def __init__(self, patterns):
            self._regexes = [re.compile(p) for p in patterns]
        def exclude(self, url: str) -> bool:
            return any(rx.search(url) for rx in self._regexes)

    exclude_filter = ExcludePatternFilter([r"/pdf/", r"\.pdf$", r"\.jpg$", r"\.png$", r"\.gif$"])
    seen, queue = set(), deque([(root_url, 0)])
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        while queue:
            url, depth = queue.popleft()
            if url in seen or depth >= max_crawl_depth:
                continue
            seen.add(url)

            try:
                logger.debug(f"Crawling URL (depth {depth}): {url}")
                html = await _fetch_with_fallback(url, client, sem)
            except Exception:
                # already logged in helper
                continue

            base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"].split("#")[0]
                if not href or href.startswith(("mailto:", "tel:")):
                    continue

                full = urljoin(base, href)
                if not _inside_start_path(full) and not include_external_links:
                    continue
                if exclude_filter.exclude(full):
                    continue

                if full not in seen:
                    queue.append((full, depth + 1))

    return seen

# ———————————————————————————————————————————————————————————————
# fetch helpers
# ———————————————————————————————————————————————————————————————

async def _fetch_with_fallback(url: str, client: httpx.AsyncClient, sem: asyncio.Semaphore) -> str:
    """
    Try httpx GET first; on RequestError or certain status codes, 
    fall back to Crawl4AI Playwright render.
    """
    try:
        return await _fetch_static(url, client, sem)
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        code = getattr(e, "response", None) and e.response.status_code
        # treat connect/read timeouts, 403,404,429 as retryable
        if isinstance(e, httpx.RequestError) or code in {403, 404, 429}:
            logger.warning(f"Falling back to Playwright for {url}: {str(e)}")
            try:
                return await _fetch_dynamic(url)
            except Exception as de:
                logger.error(f"Playwright fetch failed for {url}: {de}")
        else:
            logger.warning(f"Non-retryable HTTP error for {url}: {str(e)}")
        raise

async def _fetch_static(url: str, client: httpx.AsyncClient, sem: asyncio.Semaphore) -> str:
    """Simple httpx fetch with semaphore for concurrency control."""
    async with sem:
        resp = await client.get(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml"
        })
        resp.raise_for_status()
        return resp.text

async def _fetch_dynamic(url: str) -> str:
    """
    Use Crawl4AI's Playwright strategy to render JS and return fully
    hydrated HTML.  Headless by default.
    """
    strategy = AsyncPlaywrightCrawlerStrategy(headless=True)
    crawler = AsyncWebCrawler(crawler_strategy=strategy)
    result = await crawler.arun([{"url": url}])
    # result is a list of dicts; each has a .response.html field
    html = result.html
    if not html:
        raise RuntimeError("Empty HTML from dynamic fetch")
    return html
