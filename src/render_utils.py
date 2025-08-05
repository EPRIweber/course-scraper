# src/render_utils.py
import asyncio
import logging
import random
from typing import Optional

import httpx
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy

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
    """Render ``url`` using Playwright via Crawl4AI."""
    logger.debug("Dynamic fetch for URL: %s", url)
    crawler, _ = _get_playwright_crawler()
    result = await crawler.arun(url=url)
    html = result.html or ""
    if not html:
        raise RuntimeError("Empty HTML from dynamic fetch")
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


async def fetch_with_fallback(url: str, client: httpx.AsyncClient, sem: asyncio.Semaphore, *, delay: float = 1.0, default_playwright: bool = False) -> str:
    """Fetch page HTML with HTTPX, falling back to Playwright on errors."""
    try:
        if default_playwright:
            return await fetch_dynamic(url)
        else:
            return await fetch_static(url, client, sem, delay=delay)
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        code = getattr(e, "response", None) and e.response.status_code
        if isinstance(e, httpx.RequestError) or code in {403, 404, 429}:
            logger.warning("Falling back to Playwright for %s: %s", url, e)
            try:
                return await fetch_dynamic(url)
            except Exception as de:
                logger.error("Playwright fetch failed for %s: %s", url, de)
        else:
            logger.warning("Non-retryable HTTP error for %s: %s", url, e)
        raise


async def fetch_page(url: str, *, timeout: int = 60000 * 10, delay: float = 1.0, default_playwright: bool = False) -> str:
    """Fetch ``url`` with fallback, creating a temporary HTTPX client."""
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, verify=False) as client:
        sem = asyncio.Semaphore(1)
        return await fetch_with_fallback(url, client, sem, delay=delay, default_playwright=default_playwright)
