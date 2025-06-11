# src/crawler.py
import re
from typing import List, Set, Union
from urllib.parse import urlparse

from crawl4ai import (
    AsyncWebCrawler,
    BFSDeepCrawlStrategy,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    DomainFilter,
    FilterChain,
    URLFilter,
)

from src.config import SourceConfig

class ExcludePatternFilter(URLFilter):
    def __init__(self, patterns: list[str]):
        self._regexes = [re.compile(p) for p in patterns]

    def apply(self, url: str) -> bool:
        for rx in self._regexes:
            if rx.search(url):
                return False
        return True


async def BFS_crawl(
    root_url: Union[str, object],
    max_crawl_depth: int = 5,
    include_external_links: bool = False,
    page_timeout_ms: int = 5000,
    word_count_min: int = 10
) -> Set[str]:
    root = str(root_url)
    domain = urlparse(root).netloc

    exclude = ExcludePatternFilter([
        r"/pdf/", r"\.pdf$", r"/archive/", r"/search/"
    ])

    filters = FilterChain([
        DomainFilter(allowed_domains=[domain]),
        exclude
    ])

    strategy = BFSDeepCrawlStrategy(
        max_depth=max_crawl_depth,
        include_external=include_external_links,
        filter_chain=filters,
    )

    browser_cfg = BrowserConfig(headless=True, verbose=False)
    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        stream=True,
        page_timeout=page_timeout_ms,
        word_count_threshold=word_count_min,
        deep_crawl_strategy=strategy,
    )

    seen: Set[str] = set()
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        # pass root as str to avoid HttpUrl in CrawlResult
        async for page in await crawler.arun(url=root, config=run_cfg):
            # record the page itself
            if page.url and page.url not in seen:
                seen.add(page.url)

            # collect internal + optional external links
            links = page.links.get("internal", [])
            if include_external_links:
                links += page.links.get("external", [])
            for link in links:
                href = link.get("href")
                if href and href not in seen:
                    seen.add(href)

    return seen

async def crawl_and_collect_urls(source: SourceConfig) -> List[str]:
    urls = await BFS_crawl(
        root_url=source.root_url,
        max_crawl_depth=source.crawl_depth,
        include_external_links=source.include_external,
        page_timeout_ms=source.page_timeout_ms,
        word_count_min=10
    )
    return urls
