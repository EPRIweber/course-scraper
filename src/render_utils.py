# src/render_utils.py
import asyncio
import logging
import random
from typing import Optional

import httpx
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
from crawl4ai import CrawlerRunConfig, VirtualScrollConfig

logger = logging.getLogger(__name__)

_strategy: AsyncPlaywrightCrawlerStrategy | None = None
_crawler: AsyncWebCrawler | None = None


def _get_playwright_crawler() -> tuple[AsyncWebCrawler, AsyncPlaywrightCrawlerStrategy]:
    """Return a shared Crawl4AI crawler/strategy pair."""
    global _strategy, _crawler
    if not _strategy:
        _strategy = AsyncPlaywrightCrawlerStrategy(headless=True, logger=logger)
        _crawler = AsyncWebCrawler(crawler_strategy=_strategy)
    return _crawler, _strategy


async def close_playwright() -> None:
    """Close the shared Playwright strategy."""
    if _strategy:
        await _strategy.close()

async def fetch_dynamic(url: str) -> str:
    """
    Render ``url`` using Playwright via Crawl4AI with virtual-scroll enabled,
    so that *all* links—even in client-rendered or windowed lists—end up in the HTML.
    """
    crawler, _ = _get_playwright_crawler()

    # Configure Virtual Scroll to scroll the full page container ("body").
    vs_config = VirtualScrollConfig(
        container_selector="body",    # scroll the main viewport
        scroll_count=30,              # number of scroll steps; bump if you're still missing items
        scroll_by="page_height",      # scroll by the viewport height each time
        wait_after_scroll=0.5         # half-second pause for content to render
    )

    run_cfg = CrawlerRunConfig(
        wait_for="body",                 # don't grab HTML until <body> is loaded
        virtual_scroll_config=vs_config  # hook in the virtual scroll behavior
    )

    result = await crawler.arun(url=url, run_config=run_cfg)
    html = result.html or ""
    if not html:
        raise RuntimeError(f"Empty HTML after virtual scrolling render of {url}")
    return html

async def fetch_static(url: str, client: httpx.AsyncClient, sem: asyncio.Semaphore, *, delay: float = 1.0) -> str:
    """Return HTML using ``client`` with retry/backoff on certain errors."""
    backoff = 1.0
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        async with sem:
            resp = await client.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
        if resp.status_code < 400:
            html = resp.text
            await asyncio.sleep(delay + random.random())
            return html
        if resp.status_code not in (403, 429, 503):
            resp.raise_for_status()

        logger.warning(
            "Received %d from %s, backing off %.1fs (attempt %d/%d)",
            resp.status_code,
            url,
            backoff,
            attempt,
            max_retries,
        )
        await asyncio.sleep(backoff + random.random())
        backoff *= 2

    async with sem:
        resp = await client.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
    resp.raise_for_status()
    html = resp.text
    await asyncio.sleep(delay + random.random())
    return html

def _looks_like_cloudflare(html: str) -> bool:
    """Heuristic: detect Cloudflare interstitial/challenge pages."""
    if not html:
        return False
    h = html.lower()
    markers = (
        "cloudflare.com",
        "/cdn-cgi/",
        "cf-chl-",
        "cf-ray",
        "cf-browser-verification",
        "just a moment",
        "attention required",
        "utm_source=challenge",
    )
    return any(m in h for m in markers)

async def fetch_with_fallback(
    url: str,
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    *,
    delay: float = 1.0,
    default_playwright: bool = False
) -> str:
    """
    Fetch page HTML, defaulting to Playwright. If a Cloudflare interstitial is detected,
    back off briefly and re-fetch via Playwright (up to two extra attempts).
    If Playwright itself fails, fall back to static HTTPX (with its own backoff).
    """
    # --- 1) Try Playwright first ---
    try:
        html = await fetch_dynamic(url)
    except Exception as e:
        logger.warning("Playwright failed for %s (%s). Falling back to static HTTP.", url, e)
        html = await fetch_static(url, client, sem, delay=delay)
        # If static returns a CF interstitial, escalate back to Playwright once.
        if _looks_like_cloudflare(html):
            logger.info("Cloudflare detected after static fetch for %s; retrying with Playwright", url)
            await asyncio.sleep(1.0 + random.random() * 0.5)
            html = await fetch_dynamic(url)
        return html

    # --- 2) If Playwright succeeded but we hit CF, retry with small backoffs ---
    if _looks_like_cloudflare(html):
        logger.info("Cloudflare interstitial detected for %s; backing off and reloading via Playwright", url)
        # First CF retry
        await asyncio.sleep(1.0 + random.random() * 0.5)
        html = await fetch_dynamic(url)

        # Second CF retry if needed
        if _looks_like_cloudflare(html):
            logger.info("Cloudflare still present for %s; retrying once more after short backoff", url)
            await asyncio.sleep(2.0 + random.random() * 0.5)
            html = await fetch_dynamic(url)

    return html

async def fetch_page(
    url: str,
    *,
    timeout: int = 60000 * 10,
    delay: float = 1.0,
    default_playwright: bool = False
) -> str:
    """Fetch ``url`` using Playwright-first strategy with CF-aware backoff."""
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, verify=False) as client:
        sem = asyncio.Semaphore(1)
        return await fetch_with_fallback(url, client, sem, delay=delay, default_playwright=default_playwright)
