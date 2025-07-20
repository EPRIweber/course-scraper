import argparse
import asyncio
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

BING_API_ENDPOINT = "https://api.bing.microsoft.com/v7.0/search"
BING_API_KEY = os.getenv("BING_API_KEY")

KEYWORDS = ["catalog", "bulletin", "courses", "curriculum"]


async def bing_search(query: str, *, count: int = 5) -> List[str]:
    """Return a list of result URLs from the Bing Web Search API."""
    if not BING_API_KEY:
        raise RuntimeError("BING_API_KEY environment variable is required")
    headers = {"Ocp-Apim-Subscription-Key": BING_API_KEY}
    params = {"q": query, "count": count, "responseFilter": "Webpages"}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(BING_API_ENDPOINT, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    return [item["url"] for item in data.get("webPages", {}).get("value", [])]


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
    query = f"{school} course catalog site:edu"
    try:
        results = await bing_search(query)
    except Exception as e:
        print(f"Search failed for {school}: {e}")
        return None
    candidates = filter_catalog_urls(results)
    for url in candidates:
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


def update_sources_file(path: Path, new_sources: List[SourceConfig], dry_run: bool = False) -> None:
    if path.exists():
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}
    existing = data.get("sources", [])
    for src in new_sources:
        existing.append(src.model_dump(exclude_defaults=True, exclude_none=True))
    data["sources"] = existing
    if dry_run:
        print(yaml.safe_dump(data, sort_keys=False))
        return
    with open(path, "w") as f:
        yaml.safe_dump(data, f, sort_keys=False)
    print(f"Updated {path}")


def load_names_from_csv(csv_path: Path) -> List[str]:
    with open(csv_path) as f:
        return [row[0] for row in csv.reader(f) if row]


async def async_main(args: argparse.Namespace) -> None:
    names: List[str] = []
    if args.school:
        names.append(args.school)
    if args.csv:
        names.extend(load_names_from_csv(Path(args.csv)))
    if not names:
        print("No school names provided")
        return

    sources = await generate_for_schools(names)
    if not sources:
        print("No sources generated")
        return
    update_sources_file(Path(args.out), sources, dry_run=args.dry_run)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate SourceConfig entries automatically")
    parser.add_argument("--school", help="School name to search")
    parser.add_argument("--csv", help="CSV file containing school names")
    parser.add_argument("--out", default=str(Path(__file__).parent.parent / "configs/sources.yaml"), help="Path to sources.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Print configs instead of writing file")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
