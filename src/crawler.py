# src/crawler.py
import asyncio
from collections import deque
import re
from pathlib import Path
from typing import List, Set, Union
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from src.config import SourceConfig


class ExcludePatternFilter:
    def __init__(self, patterns: List[str]):
        self._regexes = [re.compile(p) for p in patterns]

    def exclude(self, url: str) -> bool:
        """Return True if url matches any exclude pattern."""
        return any(rx.search(url) for rx in self._regexes)


async def static_bfs_crawl(
    root_url: Union[str, object],
    max_crawl_depth: int = 5,
    include_external_links: bool = False,
    concurrency: int = 20
) -> Set[str]:
    root = str(root_url)
    domain = urlparse(root).netloc
    exclude_filter = ExcludePatternFilter([
        r"/pdf/", r"\.pdf$", r"/archive/", r"/search/"
    ])

    seen: Set[str] = set()
    queue = deque([(root, 0)])
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=10) as client:
        async def fetch(url: str) -> str:
            async with sem:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.text

        while queue:
            url, depth = queue.popleft()
            if url in seen:
                continue
            seen.add(url)

            if depth >= max_crawl_depth:
                continue

            try:
                html = await fetch(url)
            except Exception:
                # skip pages that fail
                continue

            base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"].split("#")[0]
                full = urljoin(base, href)

                # domain filter
                if not include_external_links and urlparse(full).netloc != domain:
                    continue

                # exclude patterns
                if exclude_filter.exclude(full):
                    continue

                if full not in seen:
                    queue.append((full, depth + 1))

    return seen


async def crawl_and_collect_urls(source: SourceConfig) -> List[str]:
    """
    Crawl the site starting at source.root_url, using a static HTTP+BS4 BFS.
    Returns a sorted list of unique URLs.
    """
    urls = await static_bfs_crawl(
        root_url=source.root_url,
        max_crawl_depth=source.crawl_depth,
        include_external_links=source.include_external,
        concurrency=source.max_concurrency
    )
    return sorted(urls)










# import re
# from typing import List, Set, Union
# from urllib.parse import urlparse

# from crawl4ai import (
#     AsyncWebCrawler,
#     BFSDeepCrawlStrategy,
#     BrowserConfig,
#     CacheMode,
#     CrawlerRunConfig,
#     DomainFilter,
#     FilterChain,
#     URLFilter,
# )

# from src.config import SourceConfig

# class ExcludePatternFilter(URLFilter):
#     def __init__(self, patterns: list[str]):
#         self._regexes = [re.compile(p) for p in patterns]

#     def apply(self, url: str) -> bool:
#         for rx in self._regexes:
#             if rx.search(url):
#                 return False
#         return True


# async def BFS_crawl(
#     root_url: Union[str, object],
#     max_crawl_depth: int = 5,
#     include_external_links: bool = False,
#     page_timeout_s: int = 5000,
#     word_count_min: int = 10
# ) -> Set[str]:
#     root = str(root_url)
#     domain = urlparse(root).netloc

#     exclude = ExcludePatternFilter([
#         r"/pdf/", r"\.pdf$", r"/archive/", r"/search/"
#     ])

#     filters = FilterChain([
#         DomainFilter(allowed_domains=[domain]),
#         exclude
#     ])

#     strategy = BFSDeepCrawlStrategy(
#         max_depth=max_crawl_depth,
#         include_external=include_external_links,
#         filter_chain=filters,
#     )

#     browser_cfg = BrowserConfig(headless=True, verbose=False)
#     run_cfg = CrawlerRunConfig(
#         cache_mode=CacheMode.BYPASS,
#         stream=True,
#         page_timeout=page_timeout_s,
#         word_count_threshold=word_count_min,
#         deep_crawl_strategy=strategy,
#     )

#     seen: Set[str] = set()
#     async with AsyncWebCrawler(config=browser_cfg) as crawler:
#         # pass root as str to avoid HttpUrl in CrawlResult
#         async for page in await crawler.arun(url=root, config=run_cfg):
#             # record the page itself
#             if page.url and page.url not in seen:
#                 seen.add(page.url)

#             # collect internal + optional external links
#             links = page.links.get("internal", [])
#             if include_external_links:
#                 links += page.links.get("external", [])
#             for link in links:
#                 href = link.get("href")
#                 if href and href not in seen:
#                     seen.add(href)

#     return seen

# async def crawl_and_collect_urls(source: SourceConfig) -> List[str]:
#     urls = await BFS_crawl(
#         root_url=source.root_url,
#         max_crawl_depth=source.crawl_depth,
#         include_external_links=source.include_external,
#         page_timeout_s=source.page_timeout_s,
#         word_count_min=10
#     )
#     return urls
