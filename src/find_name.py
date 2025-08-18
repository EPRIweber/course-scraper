# src/find_name.py

from __future__ import annotations

import logging
import re
from typing import Optional

from bs4 import BeautifulSoup

from .config import SourceConfig
from .render_utils import fetch_page

logger = logging.getLogger(__name__)

# Matches "... - {School Name} - Modern Campus Catalog™" (™/®/&trade; optional)
_MC_NAME_RE = re.compile(
    r"-\s*(?P<name>.+?)\s*-\s*Modern\s+Campus\s+Catalog(?:\s*(?:ACMS))?(?:\s*(?:™|®|&trade;|&#8482;))?",
    re.IGNORECASE,
)

def _normalize_space(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split())

async def find_site_name(
    source: SourceConfig,
) -> Optional[str]:
    async def _log(msg: str):
        logger.info(f"[{source.name}] {msg}")

    target = (getattr(source, "clean_name", None) or source.name or "").strip()
    if not target:
        await _log("No cleaned name / name present.")
        return None

    url = source.root_url or source.schema_url
    if not url:
        await _log("No URL on source.")
        return None

    try:
        html = await fetch_page(str(url), default_playwright=True)
    except Exception as e:
        await _log(f"Fetch failed for {url}: {e}")
        return None

    soup = BeautifulSoup(html or "", "html.parser")

    # Build a small set of candidates to search (title/meta), then fall back to full HTML/text
    candidates: list[str] = []
    if soup.title and soup.title.string:
        candidates.append(soup.title.string)

    # Common meta title locations on MC sites
    for sel, attr in [
        ('meta[property="og:title"]', "content"),
        ('meta[name="title"]', "content"),
        ('meta[name="twitter:title"]', "content"),
    ]:
        tag = soup.select_one(sel)
        if tag and tag.get(attr):
            candidates.append(tag.get(attr) or "")

    # Add full serialized HTML and visible text last, as fallbacks
    candidates.extend([str(soup), soup.get_text(" ", strip=True)])

    for text in candidates:
        m = _MC_NAME_RE.search(text or "")
        if m:
            name = _normalize_space(m.group("name"))
            if name:
                await _log(f"Detected site name: {name!r}")
                return name

    await _log("Modern Campus site name not found via pattern.")
    return None


# --- optional: smoke test ---
async def _smoke_test():
    """
    A quick smoke test to ensure the find_site_name function works as expected.
    Replace the URL with a real Modern Campus catalog page if you want to try live.
    """
    source = SourceConfig(
        name="Test Source",
        root_url="https://example.com",     # replace with an MC catalog URL for a real test
        schema_url="https://example.com/schema.json",
        clean_name="Example Site"
    )
    site_name = await find_site_name(source)
    logger.info(f"Smoke test site_name: {site_name}")
    return site_name

if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_smoke_test())
