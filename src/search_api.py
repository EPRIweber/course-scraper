# src/search_api.py

from __future__ import annotations

import asyncio
import os
import logging
from typing import List, Tuple

import httpx
from httpx import HTTPStatusError

logger = logging.getLogger(__name__)

GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX = os.getenv("GOOGLE_CX")

GOOGLE_API_KEY_01 = os.getenv("GOOGLE_API_KEY_01")
GOOGLE_CX_01 = os.getenv("GOOGLE_CX_01")

_GOOGLE_SEARCH_SEM = asyncio.BoundedSemaphore(1)

async def run_school_search(school: str, *, count: int = 5) -> Tuple[List[str], List[List[str]]]:
    """
    Build the queries for a school and run Google search for each.
    Returns (queries, results_by_query).
    """
    queries = [
        f"{school} course description current catalog",
      # f"{school} undergraduate course description current catalog",
      # f"{school} graduate course description current catalog",
    ]
    results: List[List[str]] = []
    for q in queries:
        try:
            hits = await google_search(q, count=count)
        except Exception as e:
            logger.warning("Search failed for %s with %r: %s", school, q, e)
            hits = []
        results.append(hits)
    return queries, results

async def google_search(query: str, *, count: int = 5) -> List[str]:
    """Thin async wrapper over Google Programmable Search with quota fallback."""
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        raise RuntimeError("GOOGLE_API_KEY and GOOGLE_CX environment variables are required")

    async with _GOOGLE_SEARCH_SEM:
        async with httpx.AsyncClient(timeout=60000 * 10, verify=False) as client:
            async def _call(key: str, cx: str) -> dict:
                params = {"key": key, "cx": cx, "q": query, "num": count}
                resp = await client.get(GOOGLE_CSE_ENDPOINT, params=params)
                resp.raise_for_status()
                return resp.json()

            try:
                data = await _call(GOOGLE_API_KEY, GOOGLE_CX)
            except HTTPStatusError as e:
                # Detect quota/rate-limit
                status = e.response.status_code if e.response is not None else None
                reason_text = ""
                try:
                    ej = e.response.json()
                    errs = (ej.get("error", {}) or {}).get("errors", []) or []
                    reason_text = " ".join([str(er.get("reason", "")) for er in errs]).lower()
                except Exception:
                    pass

                quota_like = (
                    status in (403, 429)
                    or "limit" in reason_text
                    or "quota" in reason_text
                    or "rate" in reason_text
                )

                # Fallback only when we hit quota/rate limit and a backup key exists
                if quota_like and GOOGLE_API_KEY_01 and GOOGLE_CX_01:
                    logger.warning(
                        "Primary Google CSE key quota/rate-limited (status %s, reason=%r). Falling back.",
                        status, reason_text or None
                    )
                    try:
                        data = await _call(GOOGLE_API_KEY_01, GOOGLE_CX_01)
                    except HTTPStatusError as e2:
                        logger.warning(
                            "Fallback Google Search HTTP %s for query %r: %s",
                            e2.response.status_code if e2.response else "?", query, e2
                        )
                        return []
                else:
                    logger.warning(
                        "Google Search HTTP %s for query %r: %s",
                        status if status is not None else "?", query, e
                    )
                    return []

        return [item["link"] for item in data.get("items", []) if item.get("link")]
