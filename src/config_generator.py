# src/config_generator.py
import argparse
import asyncio
from collections import OrderedDict
import csv
import json
import os
from pathlib import Path
from typing import List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup

from .render_utils import fetch_page
import yaml

from .config import SourceConfig

GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX = os.getenv("GOOGLE_CX")

KEYWORDS = ["catalog", "bulletin", "courses", "curriculum"]

async def google_search(query: str, *, count: int = 5) -> List[str]:
    """Return a list of result URLs from Google Programmable Search."""
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        raise RuntimeError("GOOGLE_API_KEY and GOOGLE_CX environment variables are required")
    params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": query, "num": count}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(GOOGLE_CSE_ENDPOINT, params=params)
        resp.raise_for_status()
        data = resp.json()
    return [item["link"] for item in data.get("items", [])]

def filter_catalog_urls(urls: List[str]) -> List[str]:
    filtered = []
    for url in urls:
        lower = url.lower()
        if any(k in lower for k in KEYWORDS) and ".edu" in lower:
            filtered.append(url)
    return filtered

async def fetch_html(url: str) -> str:
    return await fetch_page(url, timeout=10000)

def find_course_link(html: str, base_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        lower = href.lower()
        if any(k in lower for k in ["preview_course", "courses", "coursedog"]):
            return httpx.URL(href, base=base_url).human_repr()
    return None

async def analyze_candidate(url: str) -> Optional[Tuple[str, str]]:
    try:
        html = await fetch_html(url)
    except Exception:
        return None
    course_url = find_course_link(html, url)
    if course_url:
        return url, course_url
    return None

async def discover_catalog_urls(school: str) -> Optional[Tuple[str, str]]:
    query = f"{school} course description catalog bulletin site"
    try:
        results = await google_search(query)
    except Exception as e:
        print(f"Search failed for {school}: {e}")
        return None
    candidates = filter_catalog_urls(results)
    combined = candidates + results
    ordered_by_priority = list(OrderedDict.fromkeys(combined))
    for url in ordered_by_priority:
        result = await analyze_candidate(url)
        if result:
            return result
    return None

def create_source(name: str, root_url: str, schema_url: str) -> SourceConfig:
    return SourceConfig(
        source_id=f"LOCAL_{name}",
        name=name,
        root_url=root_url,
        schema_url=schema_url,
    )

async def generate_for_schools(names: List[str]) -> List[SourceConfig]:
    sources: List[SourceConfig] = []
    for name in names:
        print(f"Discovering catalog for {name}...")
        res = await discover_catalog_urls(name)
        if not res:
            print(f"  no catalog found")
            continue
        root_url, schema_url = res
        src = create_source(name, root_url, schema_url)
        sources.append(src)
        print(f"  found: {root_url} -> {schema_url}")
    return sources

def update_sources_file(new_sources: List[SourceConfig]) -> None:
    data = {}
    existing = data.get("sources", [])
    for src in new_sources:
        existing.append(src.model_dump(exclude_defaults=True, exclude_none=True))
    data["sources"] = existing
    print(yaml.safe_dump(data, sort_keys=False))
    with open('test_source_generation.yaml', "w") as f:
        yaml.safe_dump(data, f, sort_keys=False)

def load_names_from_csv(csv_path: Path) -> List[str]:
    with open(csv_path) as f:
        return [row[0] for row in csv.reader(f) if row]

async def async_main() -> None:
    names = []
    with open('configs/new_schools.csv', 'r', newline='') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            names.append(row)

    sources = await generate_for_schools(names)
    if not sources:
        print("No sources generated")
        return
    update_sources_file(sources)

def main() -> None:
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
