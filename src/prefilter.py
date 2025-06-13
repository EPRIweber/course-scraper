# src/crawler_prefilter.py
import asyncio
import httpx
from typing import List

from src.config import SourceConfig

async def filter_urls(
    urls: List[str],
    max_concurrency: int = 10,
    timeout: float = 10.0
) -> List[str]:
    """
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

async def prefilter_urls(
    urls: List[str],
    source: SourceConfig
) -> List[str]:
    concurrency = source.max_concurrency or (source.crawl_depth * 5)
    return await filter_urls(
        urls=urls,
        max_concurrency=concurrency,
        timeout=source.page_timeout_s
    )