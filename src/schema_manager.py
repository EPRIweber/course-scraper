# src/schema_manager.py
"""Schema generation and validation utilities.

This module uses LLM assistance to derive a scraping schema from a catalog page
and provides helpers to validate that schema by performing a test scrape.
"""

from playwright.async_api import Error as PlaywrightError
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
from crawl4ai import AsyncWebCrawler
from urllib.parse import urljoin
import httpx
import json, logging

from pydantic import HttpUrl
import urllib3
from src.config import SourceConfig, ValidationCheck
from crawl4ai.content_filter_strategy import PruningContentFilter
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import warnings
import urllib3

from src.llm_client import LlamaModel, GemmaModel
from src.prompts.schema import FindRepeating
from src.scraper import scrape_urls
from src.render_utils import fetch_page

REQUIRED_FIELDS = ["course_title", "course_description"]
OPTIONAL_FIELDS = ["course_code", "course_credits"]

async def generate_schema(
    source: SourceConfig,
) -> tuple[dict, int]:
    log = logging.getLogger(__name__)
    schema, usage = await _generate_schema_from_llm(
        url=source.schema_url,
        page_timout=source.page_timeout_s
    )
    log.info(f"Generated schema for {source.name!r}:\n{schema}")
    return schema, usage

# Suppress “InsecureRequestWarning” across this module
warnings.filterwarnings(
    "ignore",
    category=urllib3.exceptions.InsecureRequestWarning
)

# async def _fetch_and_expand(base_url: str, html: str) -> str:
#     soup = BeautifulSoup(html, "lxml")
#     anchors = [
#         a for a in soup.find_all("a", href=True, onclick=True)
#         if "preview_course_nopop.php" in a["href"]
#     ]

#     async with httpx.AsyncClient(timeout=10, follow_redirects=True, verify=False) as client:
#         tasks = [client.get(urljoin(base_url, a["href"])) for a in anchors]
#         responses = await asyncio.gather(*tasks, return_exceptions=True)

#     expanded_parts: list[str] = []
#     for resp in responses:
#         if isinstance(resp, Exception) or resp.status_code >= 400:
#             continue

#         frag = BeautifulSoup(resp.text, "lxml")
#         expanded_parts.append(str(frag))
#         # try pulling out exactly the course-detail cell
#         # if cells:
#         #     for cell in cells:
#         #         expanded_parts.append(str(cell))
#         # else:
#         #     # fallback to entire fragment
#         #     expanded_parts.append(str(frag))

#     # if you only want the first N courses, you can slice here
#     # expanded_parts = expanded_parts[0:10]
#     sudo_html = (
#         "<div class=\"expanded-course-details\">\n"
#         + "\n".join(expanded_parts)
#         + "\n</div>"
#     )
#     # with open(f"{base_url.replace("/", "").replace(":", "").replace("?", "").replace("=", "").replace("&", "")}", "w") as f:
#     #     f.write(sudo_html)

#     cells = BeautifulSoup(sudo_html, "lxml").select("td.coursepadding")

#     return (
#         "<div class=\"expanded-course-details\">\n"
#         + "\n".join(cells)
#         + "\n</div>"
#     )

async def _generate_schema_from_llm(
    url: HttpUrl,
    page_timout: int,
) -> tuple[dict, int]:
    """Helper function to perform LLM call."""
    log = logging.getLogger(__name__)

    # unified fetch + fallback
    try:
        catalog_html = await fetch_page(str(url), timeout=60000*10)
    except Exception as e:
        raise RuntimeError(f"Failed to load schema_url {url}: {e}")

    # print(catalog_html)

    if "Modern Campus Catalog" in catalog_html:
        # raw_html = await _fetch_and_expand(str(url), catalog_html)
        with open("src/modern_campus.json", 'r') as f:
            return json.load(f), 0
    else:
        raw_html = catalog_html
    soup = BeautifulSoup(raw_html, "lxml")
    html_snippet = soup.encode_contents().decode()

    # 2) Prune until snippet is reasonably small (or threshold too high)
    prune_threshold = 0.0
    html_for_schema = html_snippet
    while len(html_for_schema) > 250_000 and prune_threshold < 1.0:
        prune_threshold += 0.1
        pruner = PruningContentFilter(threshold=prune_threshold)
        chunks = pruner.filter_content(html_snippet)
        html_for_schema = "\n".join(chunks)

    # print(html_for_schema)

    # print(html_for_schema)
    log.info(
        "Generating schema with %d characters (prune_threshold=%.1f) from %s",
        len(html_for_schema), prune_threshold, url
    )

    prompt: FindRepeating = FindRepeating(
        role="You specialize in exacting structured course data from course catalog websites.",
        repeating_block="course block",
        repeating_item="course",
        required_fields=REQUIRED_FIELDS,
        optional_fields=OPTIONAL_FIELDS,
        html=html_for_schema,
        type="css",
        target_json_example=json.dumps([{
            "course_title": "Biochemistry",
            "course_description": "Lectures and recitation sections explore the structure and function of biological molecules, including proteins, nucleic acids, carbohydrates, and lipids. Topics include enzyme kinetics, metabolic pathways, and the molecular basis of genetic information.",
            "course_code": "BIOL 0280",
            "course_credits": "4 Credits"
        }], indent=2)
    )

    # llm = GemmaModel()
    llm = LlamaModel()
    llm.set_response_format({
        "type": "json_object",
        "json_schema": {
            "name": "CourseExtractionSchema",
            "description": "CourseExtractionSchema",
            "schema": {
                "type": "object",
                "properties": {
                    "name":          {"type": "string"},
                    "baseSelector":  {"type": "string"},
                    "baseFields": {
                        "type":     "array",
                        "items":    {"type": "object"}
                    },
                    "fields": {
                        "type":     "array",
                        "items":    {"type": "object"}
                    }
                },
                "required": ["name", "baseSelector", "fields"]
            },
            "strict": True
        }
    })

    response = llm.chat(
        messages=[
            {"role":"system", "content": prompt.system()},
            {"role":"user",   "content": prompt.user()},
        ],
        max_tokens=30000,
        temperature=0.0
    )

    content = response["choices"][0]["message"]["content"]
    obj = json.loads(content)
    if isinstance(obj, list):
        if len(obj) == 1:
            obj = obj[0]
        else:
            raise ValueError("LLM returned an array; expected a single schema object")

    prompt_t = response.get("usage", {}).get("prompt_tokens")
    completion_t = response.get("usage", {}).get("completion_tokens")

    try:
        return obj, prompt_t + completion_t
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse schema JSON:\n{content}") from e

async def validate_schema(
    schema: dict,
    source: SourceConfig
) -> tuple[ValidationCheck, str]:
    """
    Quickly sanity-check a freshly-generated schema against
    ``source.schema_url``.

    Returns
    -------
    ValidationCheck
        valid == True  -> safe to persist the schema
        valid == False -> see .fields_missing / .errors for details
    """
    log = logging.getLogger(__name__)

    required_fields = REQUIRED_FIELDS
    fields_missing: list[str] = []
    errors: list[str] = []

    # with open("src/modern_campus.json", 'r') as f:
    #     modern_campus_schema = json.load(f)
    # if schema == modern_campus_schema:
    #     log.info("Skipping modern campus schema")
    #     return ValidationCheck(False, [], [])

    at_least_one_good = False
    output = None

    try:
        # Scrape just the schema_url page
        records, _, _, json_errors = await scrape_urls(
            urls=[str(source.schema_url)],
            schema=schema,
            source=source
        )
        

        if not records:
            log.warning(f"No records returned for {source.name}")
        else:
            sample = json.dumps(records[0], indent=4)
            output = f"Sample record for schema validation:\n{sample}"

        # surface JSON decode errors, if any
        if json_errors:
            errors.extend(json_errors)

        if not records:
            errors.append("No records extracted from the test page.")
        else:
            # check that each required field appears at least once


            for rec in records:
                good_rec = True
                if isinstance(rec, dict):
                    for field in required_fields:
                        if field not in rec or not rec.get(field) or not len(rec.get(field)) > 0:
                            good_rec = False
                if good_rec:
                    at_least_one_good = True

            # for field in required_fields:
            #     if not any(isinstance(rec, dict) and field in rec and rec.get(field) for rec in records):
            #         fields_missing.append(field)

    except Exception as exc:
        log.exception("Schema validation failed")
        errors.append(str(exc))

    valid = at_least_one_good
    return ValidationCheck(
        valid=valid,
        fields_missing=fields_missing,
        errors=errors,
        output=output
    )
