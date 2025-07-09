# src/crawler.py
import asyncio
from collections import deque
import re
from typing import Set
from urllib.parse import urljoin, urlparse
import logging
import random
import ssl
import warnings
import httpx
from bs4 import BeautifulSoup

from crawl4ai import AsyncWebCrawler
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
import urllib3

from .config import SourceConfig

logging.getLogger("src.crawler").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

_strategy: AsyncPlaywrightCrawlerStrategy | None = None
_crawler: AsyncWebCrawler | None = None

# ———————————————————————————————————————————————————————————————
# public entrypoint
# ———————————————————————————————————————————————————————————————

async def crawl_and_collect_urls(source: SourceConfig) -> list[str]:
    logger.debug(f"""Running crawl with:
  max_crawl_depth:{source.crawl_depth}
  include_external:{source.include_external}
  concurrency:{source.max_concurrency}
  exclude_patterns(additional):{source.url_exclude_patterns}""")
    urls = await _static_bfs_crawl(
        root_url=str(source.root_url),
        max_crawl_depth=source.crawl_depth,
        include_external_links=source.include_external,
        concurrency=source.max_concurrency,
        exclude_patterns=source.url_exclude_patterns,
        base_exclude=str(source.url_base_exclude),
        timeout=source.page_timeout_s
    )
    return sorted(urls)

# ———————————————————————————————————————————————————————————————
# internal BFS crawl
# ———————————————————————————————————————————————————————————————

# Suppress “InsecureRequestWarning” across this module
warnings.filterwarnings(
    "ignore",
    category=urllib3.exceptions.InsecureRequestWarning
)

async def _static_bfs_crawl(
    root_url: str,
    max_crawl_depth: int,
    include_external_links: bool,
    concurrency: int,
    exclude_patterns: list[str],
    base_exclude: str,
    timeout: int
) -> Set[str]:
    start = urlparse(base_exclude if base_exclude else root_url)
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

    exclude_filter = ExcludePatternFilter(
        [r"/pdf/", r"\.pdf$", r"\.jpg$", r"\.png$", r"\.gif$"] + exclude_patterns
        if exclude_patterns
        else [r"/pdf/", r"\.pdf$", r"\.jpg$", r"\.png$", r"\.gif$"]
    )
    seen, queue = set(), deque([(root_url, 0)])
    sem = asyncio.Semaphore(concurrency)



    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        verify=False
    ) as client:
        resp = await client.get(str(root_url))
        resp.raise_for_status()
        catalog_html = resp.text
    
        if "Modern Campus Catalog" in catalog_html:
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
                    
                    if "preview_course_nopop.php" in full:
                        seen.add(full)

                for a in soup.select('tr > td[colspan="2"] > a[href]', href=True):
                    href = a['href'].split('#')[0]
                    if not href or href.startswith(("mailto:", "tel:")):
                        continue

                    full = urljoin(base, href)
                    if not _inside_start_path(full) and not include_external_links:
                        continue
                    if exclude_filter.exclude(full):
                        continue

                    if full not in seen and "content.php" in full:
                        queue.append((full, depth + 1))

        else:
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

def _get_playwright_crawler():
    global _strategy, _crawler
    if not _strategy:
        _strategy = AsyncPlaywrightCrawlerStrategy(headless=True, logger=logger)
        _crawler = AsyncWebCrawler(crawler_strategy=_strategy)
    return _crawler, _strategy

async def close_playwright():
    if _strategy:
        await _strategy.close()

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
    """Simple httpx fetch with semaphore for concurrency control, with retry/backoff on 429/503."""
    backoff = 1.0
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        async with sem:
            resp = await client.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "text/html,application/xhtml+xml"
                }
            )
        # if not a retryable status, or success, break or error
        if resp.status_code < 400:
            return resp.text
        if resp.status_code not in (403, 429, 503):
            resp.raise_for_status()

        # rate-limited or service unavailable → back off and retry
        logger.warning(
            "Received %d from %s, backing off %.1fs (attempt %d/%d)",
            resp.status_code, url, backoff, attempt, max_retries
        )
        # jitter ± 0–1s
        await asyncio.sleep(backoff + random.random())
        backoff *= 2

    # if we exhaust retries, do one final request to raise the right error
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
    logger.debug(f"Dynamic fetch for URL: {url!r}")
    crawler, _ = _get_playwright_crawler()
    result = await crawler.arun([{"url": url}])
    logger.debug("Raw playwright result: %s", result)
    # result is a list of dicts; each has a .response.html field
    html = result.html or ""
    if not html:
        raise RuntimeError("Empty HTML from dynamic fetch")
    return html
