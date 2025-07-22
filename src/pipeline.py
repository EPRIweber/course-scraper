"""Simple scraping pipeline orchestration with fallback hooks.

This module defines an orchestration function `run_scrape_pipeline` that
executes the main scraping stages with minimal error handling.  Each
public step in the pipeline has a corresponding `_foo_fallback` helper
that can be fleshed out later.  The fallbacks currently contain only
`# TODO` markers.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import List, Dict, Tuple

from .config import SourceConfig, Stage
from .crawler import crawl_and_collect_urls
# from .prefilter import prefilter_urls
from .config_generator import generate_config
from .schema_manager import validate_schema, generate_schema
from .scraper import scrape_urls
# from .classify_manager import classify_courses
from .storage import SqlServerStorage, StorageBackend

# The real implementations of these helpers are expected to exist elsewhere in
# the code base.  Local stubs keep this module importable until those modules
# are available.
async def discover_catalog_root(school: str) -> str:  # pragma: no cover - stub
    raise NotImplementedError

async def discover_schema_url(root_url: str, candidates: List[str]) -> str:  # pragma: no cover - stub
    raise NotImplementedError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# fallback helpers
# ---------------------------------------------------------------------------

async def _discover_catalog_root_fallback(school: str) -> str:
    """Fallback for `discover_catalog_root`."""
    # TODO: implement fallback logic
    return ""


async def _crawl_and_collect_urls_fallback(source: SourceConfig) -> List[str]:
    """Fallback for `crawl_and_collect_urls`."""
    # TODO: implement fallback logic
    return []


async def _prefilter_urls_fallback(urls: List[str], source: SourceConfig) -> List[str]:
    """Fallback for `prefilter_urls`."""
    # TODO: implement fallback logic
    return urls


async def _discover_schema_url_fallback(root_url: str, candidates: List[str]) -> str:
    """Fallback for `discover_schema_url`."""
    # TODO: implement fallback logic
    return root_url


async def _validate_schema_fallback(schema: dict, source: SourceConfig) -> bool:
    """Fallback for `validate_schema`."""
    # TODO: implement fallback logic
    return False


async def _scrape_urls_fallback(urls: List[str], schema: dict, source: SourceConfig) -> List[Dict]:
    """Fallback for `scrape_urls`."""
    # TODO: implement fallback logic
    return []


async def _classify_courses_fallback(courses: List[Tuple[str, str, str]]) -> List[Tuple[str, List[str]]]:
    """Fallback for `classify_courses`."""
    # TODO: implement fallback logic
    return []


async def _save_data_fallback(source_id: str, records: List[Dict]) -> None:
    """Fallback for `save_data`."""
    # TODO: implement fallback logic
    return None


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------

async def run_scrape_pipeline(
    school: str,
    run_id: int,
    storage: StorageBackend,
) -> None:
    """Run the scraping pipeline for ``school`` using provided ``storage``.

    Parameters
    ----------
    school:
        Human-readable name of the school to scrape.
    run_id:
        Current pipeline run identifier for logging.
    storage:
        Active storage backend shared by the application.
    """

    source_id = f"{school}"
    await storage.log(
        run_id,
        source_id,
        int(Stage.CRAWL),
        f"Pipeline start for {school}",
    )

    existing = await storage.list_sources()
    if any(src.name.lower() == school.lower() for src in existing):
        logger.info("%s already processed; skipping", school)
        return

    try:
        root_url = await discover_catalog_root(school)  # type: ignore
    except Exception as exc:  # noqa: F841 - allow unused for scaffolding
        logger.exception("discover_catalog_root failed: %s", exc)
        root_url = await _discover_catalog_root_fallback(school)

    source = SourceConfig(
        source_id=source_id,
        name=school,
        root_url=root_url,
        schema_url=root_url,
    )

    try:
        urls = await crawl_and_collect_urls(source)
    except Exception as exc:  # noqa: F841
        logger.exception("crawl_and_collect_urls failed: %s", exc)
        urls = await _crawl_and_collect_urls_fallback(source)

    # try:
    #     urls = await prefilter_urls(urls, source)
    # except Exception as exc:  # noqa: F841
    #     logger.exception("prefilter_urls failed: %s", exc)
    #     urls = await _prefilter_urls_fallback(urls, source)

    try:
        schema_url = await discover_schema_url(root_url, urls)  # type: ignore
    except Exception as exc:  # noqa: F841
        logger.exception("discover_schema_url failed: %s", exc)
        schema_url = await _discover_schema_url_fallback(root_url, urls)
    source.schema_url = schema_url

    schema: dict = {}
    try:
        valid = await validate_schema(schema, source)
    except Exception as exc:  # noqa: F841
        logger.exception("validate_schema failed: %s", exc)
        valid = await _validate_schema_fallback(schema, source)
    if not valid:
        logger.error("Schema validation failed for %s", school)

    try:
        records, *_ = await scrape_urls(urls, schema, source)
    except Exception as exc:  # noqa: F841
        logger.exception("scrape_urls failed: %s", exc)
        records = await _scrape_urls_fallback(urls, schema, source)

    courses = [
        (str(idx), rec.get("course_title", ""), rec.get("course_description", ""))
        for idx, rec in enumerate(records)
    ]
    # try:
    #     classified, _ = await classify_courses(courses)
    # except Exception as exc:  # noqa: F841
    #     logger.exception("classify_courses failed: %s", exc)
    #     classified = await _classify_courses_fallback(courses)
    # logger.debug("Classified %d courses", len(classified))

    try:
        await storage.save_data(source.source_id, records)
    except Exception as exc:  # noqa: F841
        logger.exception("save_data failed: %s", exc)
        await _save_data_fallback(source.source_id, records)

    await storage.log(
        run_id,
        source_id,
        int(Stage.STORAGE),
        f"Pipeline complete for {school}",
    )
    logger.info("Pipeline for %s completed", school)

