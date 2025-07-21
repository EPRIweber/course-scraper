# src/crawler.py
"""URL discovery utilities.

This module performs a breadth-first crawl starting from a source's root URL.
It collects pages that likely contain course information while respecting depth
limits and optional exclusion patterns.
"""

import asyncio
from collections import deque
import re
from typing import Set
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
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
from .render_utils import (
    fetch_with_fallback
)

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
        base_exclude=source.url_base_exclude,
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
    base_exclude: str | None,
    timeout: int
) -> Set[str]:
    start = urlparse(base_exclude or root_url)
    # print(">>> parsing:", base_exclude or root_url)
    domain = start.netloc
    root_path = (start.path.rstrip("/") + "/") if start.path else "/"
    # print(f'ROOT PATH: {root_path}')
    # print(f"START: {start}")

    rp = RobotFileParser()
    try:
        rp.set_url(urljoin(root_url, "/robots.txt"))
        rp.read()
    except Exception as e:
        logger.warning("Failed to read robots.txt: %s", e)
    delay = rp.crawl_delay("*") or 1.0

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



    limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        verify=False,
        limits=limits,
    ) as client:
        resp = await client.get(str(root_url))
        resp.raise_for_status()
        catalog_html = resp.text
    
        if "Modern Campus Catalog" in catalog_html:
            print("Modern Campus Website Detected")
            while queue:
                url, depth = queue.popleft()
                if url in seen or depth >= max_crawl_depth:
                    continue
                seen.add(url)

                try:
                    logger.debug(f"Crawling URL (depth {depth}): {url}")
                    html = await fetch_with_fallback(url, client, sem, delay=delay)
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
                    html = await fetch_with_fallback(url, client, sem, delay=delay)
                except Exception:
                    # already logged in helper
                    continue

                base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
                soup = BeautifulSoup(html, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a["href"].split("#", 1)[0]
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

# fetch helpers are provided by src.render_utils
