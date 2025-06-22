# src/crawler.py
import asyncio
from collections import deque
import re
from pathlib import Path
from typing import List, Set, Union
from urllib.parse import urljoin, urlparse
import logging

import httpx
from bs4 import BeautifulSoup

# Use relative imports for modules within the same package
from .config import SourceConfig

logger = logging.getLogger(__name__)

async def crawl_and_collect_urls(source: SourceConfig) -> List[str]:
    """
    Crawl the site starting at source.root_url, using a static HTTP+BS4 BFS.
    Returns a sorted list of unique URLs.
    """
    urls = await _static_bfs_crawl(
        root_url=str(source.root_url),
        max_crawl_depth=source.crawl_depth or 3,
        include_external_links=source.include_external or False,
        concurrency=source.max_concurrency or 10
    )
    return sorted(urls)

class ExcludePatternFilter:
    def __init__(self, patterns: List[str]):
        self._regexes = [re.compile(p) for p in patterns]

    def exclude(self, url: str) -> bool:
        """Return True if url matches any exclude pattern."""
        return any(rx.search(url) for rx in self._regexes)


async def _static_bfs_crawl(
    root_url: str,
    max_crawl_depth: int = 3,
    include_external_links: bool = False,
    concurrency: int = 10
) -> Set[str]:
    """Performs a breadth-first search crawl, returning a set of unique URLs found."""
    start = urlparse(root_url)
    domain = start.netloc
    root_path  = (start.path.rstrip("/") + "/") if start.path else "/"
    exclude_filter = ExcludePatternFilter([
        r"/pdf/", r"\.pdf$", r"/archive/", r"/search/", r"\.jpg$", r"\.png$", r"\.gif$"
    ])

    def _inside_start_path(u: str) -> bool:
        up = urlparse(u)
        return (up.netloc == domain) and up.path.startswith(root_path)

    seen: Set[str] = set()
    queue = deque([(root_url, 0)])
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        async def fetch(url: str) -> str:
            async with sem:
                resp = await client.get(url, headers={'User-Agent': 'Mozilla/5.0'})
                resp.raise_for_status()
                return resp.text

        while queue:
            url, depth = queue.popleft()
            if url in seen or depth >= max_crawl_depth:
                continue
            seen.add(url)
            
            try:
                logger.debug(f"Crawling URL (depth {depth}): {url}")
                html = await fetch(url)
            except httpx.RequestError as e:
                logger.warning(f"Request failed for {e.request.url!r} - {type(e).__name__}. Skipping.")
                continue
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP error {e.response.status_code} for {e.request.url!r}. Skipping.")
                continue
            except Exception as e:
                logger.error(f"An unexpected error occurred while fetching {url}: {e}", exc_info=False)
                continue

            base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"].split("#")[0]
                if not href or href.startswith(('mailto:', 'tel:')):
                    continue
                
                full_url = urljoin(base, href)

                if not _inside_start_path(full_url):
                    if include_external_links:
                        # If external links are allowed, skip the domain check
                        pass
                    else:
                        continue

                if exclude_filter.exclude(full_url):
                    continue

                if full_url not in seen:
                    queue.append((full_url, depth + 1))

    return seen
