# src/crawler_prefilter.py
"""Utility functions for checking URL availability before scraping."""

import asyncio
import httpx
from typing import List

from src.config import SourceConfig

async def prefilter_urls(
    urls: List[str],
    source: SourceConfig
) -> List[str]:
    """
    Prefilter URLs by checking their availability via HEAD requests.
    Returns a list of URLs that are reachable and return a 200 OK status.
    """
    if not urls:
        return []

    # If the source has a custom prefilter function, use it
    if hasattr(source, 'prefilter_function') and callable(source.prefilter_function):
        return await source.prefilter_function(urls)

    # Otherwise, use the default prefilter logic
    return await _prefilter_urls(urls, max_concurrency=source.max_concurrency, timeout=source.page_timeout_s)

async def _prefilter_urls(
    urls: List[str],
    max_concurrency: int,
    timeout: float
) -> List[str]:
    """
    prefilter_urls helper function.
    Concurrently HEAD-check each URL and return only those with a 200 OK.
    """
    sem = asyncio.Semaphore(max_concurrency)
    valid = []

    async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
        async def check(url: str):
            async with sem:
                try:
                    r = await client.head(url, follow_redirects=True)
                    # Fallback to GET on certain servers:
                    if r.status_code == 405:
                        r = await client.get(url, follow_redirects=True)
                    if r.status_code == 200:
                        valid.append(url)
                except Exception:
                    pass

        # fire off all HEAD tasks
        tasks = [asyncio.create_task(check(u)) for u in urls]
        await asyncio.gather(*tasks)

    return valid
