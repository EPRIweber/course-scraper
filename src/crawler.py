# src/crawler.py
from pathlib import Path
from crawl4ai import AsyncWebCrawler, BFSDeepCrawlStrategy, BrowserConfig, CacheMode, CrawlerRunConfig, DomainFilter, FilterChain, URLFilter
import httpx

from src.config import SourceConfig


async def BFS_crawl(
        root_url: str,
        max_crawl_depth: int = 5,
        include_external_links: bool = False,
        page_timeout_ms: int = 5000,
        word_count_min: int = 10
    ) -> list[str]:
    class ValidURLFilter(URLFilter):
        def apply(self, url: str) -> bool:
            try:
                resp = httpx.head(url, follow_redirects=True, timeout=0.5)
                if r.status_code == 405:
                    r = httpx.get(url, follow_redirects=True, timeout=0.5)
                return 200 <= resp.status_code < 400
            except Exception:
                return False

    url_filter_chain = FilterChain([
        DomainFilter(allowed_domains=["bulletin.brown.edu"]),
        ValidURLFilter()
    ])

    crawl_strategy = BFSDeepCrawlStrategy(
        max_depth=max_crawl_depth,
        include_external=include_external_links,
        filter_chain=url_filter_chain
    )

    browser_cfg = BrowserConfig(headless=True, verbose=False)
    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        stream=True,
        page_timeout=page_timeout_ms,
        word_count_threshold=word_count_min,
        deep_crawl_strategy=crawl_strategy,
        js_code=[
            """(async () => {
                const selectors = [
                    '.toggle-button', "[data-toggle='collapse']", '.accordion-button',
                    "[role='button'][aria-expanded='false']",
                ];
                for (const selector of selectors) {
                    const toggles = document.querySelectorAll(selector);
                    for (const btn of toggles) {
                        if (btn.offsetParent !== null && !btn.disabled) {
                            try { btn.click(); await new Promise(r => setTimeout(r, 150)); }
                            catch (e) { console.warn('JS click error:', e); }
                        }
                    }
                }
            })();"""
        ],
    )

    seen = set()

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        async for page in await crawler.arun(url=root_url, config=run_cfg):
            for link in page.links.get("internal", []) + page.links.get("external", []):
                href = link.get("href")
                if href and href not in seen:
                    seen.add(href)

    print(f"\nTotal unique URLs: {len(seen)}")
    return seen

async def crawl_and_collect_urls(
        source: SourceConfig
    ) -> list[str]:
    path = Path("data")/f"{source.name}_urls.txt"
    if not path.exists():
        urls = await BFS_crawl(source.root_url, depth=source.crawl_depth)
        if urls:
            with open(path, "w") as f:
                f.write("\n".join(urls))
        else:
            raise ValueError(f"Failed to crawl URLs for {source.name}.")
    return path.read_text().splitlines()