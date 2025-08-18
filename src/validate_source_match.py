# src/validate_source_match.py

from __future__ import annotations

import logging
import re
from typing import Optional, Tuple

from bs4 import BeautifulSoup

from .config import SourceConfig
from .render_utils import fetch_page

logger = logging.getLogger(__name__)


def _normalize(s: str) -> str:
    """lowercase and remove non-alphanumerics for forgiving substring match."""
    return re.sub(r'[^a-z0-9]+', '', (s or '').lower())

async def validate_source_match(
    source: SourceConfig,
) -> Tuple[bool, str]:
    """
    Fetch source.root_url (fallback: schema_url) and verify the cleaned name
    (fallback: name) appears somewhere in the rendered text.
    Returns (ok, reason) where reason is one of: 'match', 'no_match', 'no_url',
    'no_target_name', 'fetch_error: ...'
    """

    async def _log(msg: str):
        logger.info(f"[{source.name}] {msg}")

    target = (getattr(source, "clean_name", None) or source.name or "").strip()
    if not target:
        await _log("No cleaned name / name present; skipping validation.")
        return True, "no_target_name"

    url = source.root_url or source.schema_url
    if not url:
        await _log("No URL on source; cannot validate.")
        return False, "no_url"

    try:
        html = await fetch_page(str(url), default_playwright=True)
    except Exception as e:
        await _log(f"Fetch failed for {url}: {e}")
        return False, f"fetch_error: {e}"

    text = BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True)
    ok = _normalize(target) == "" or (_normalize(target) in _normalize(text))

    await _log(
        f"Validate source match {'PASS' if ok else 'FAIL'} — looked for '{target}' in {url}"
    )
    return ok, ("match" if ok else "no_match")
