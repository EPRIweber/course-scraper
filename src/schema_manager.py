# src/schema_manager.py
import json
import logging
import os
from pathlib import Path

from pydantic import HttpUrl
# Use relative imports for modules within the same package
from .config import SourceConfig
from crawl4ai import JsonCssExtractionStrategy, LLMConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from bs4 import BeautifulSoup
import requests

DEFAULT_QUERY="""
Generate a JSON schema (not the data!) using valid CSS selectors that will be used to select distinct course blocks from the given HTML.
Requirements:
- Output must be **valid JSON only** (no comments, no trailing commas).
- **Only** these keys are allowed at the top level: `"name"`, `"baseSelector"`, `"fields"`.
- **fields** are stored as an array with each field having the keys `"name"`, `"selector"`, and `"type"` with possible additional keys depending on type (i.e. attribute selectors for meta-data).
- Every course block will **ALWAYS** have the fields `"course_title"` and `"course_description"`
- A course block **MAY** contain `"course_code"`, but should only be included if it can be cleanly selected via its own CSS selector.
- The fields you may use are limited to exactly these **three** mentioned above.
**Exact JSON shape** (course_code included only if present and seperable):
{
  "name": "Course Block",
  "baseSelector": "<CSS selector, e.g. div.courseblock>",
  "fields": [
    { "name": "course_title",       "selector": "<CSS selector>", "type": "text" },
    { "name": "course_description", "selector": "<CSS selector>", "type": "text" },
    { "name": "course_code",        "selector": "<CSS selector>", "type": "text" }
  ]
}
"""

async def generate_schema(
    source: SourceConfig,
) -> dict:
    """
    Generates a scraping schema using Google's Gemini model.
    """
    log = logging.getLogger(__name__)
    log.info(f"Generating schema for {source.name!r} from URL: {source.schema_url}")
    try:
        raw = _generate_schema_from_llm(url=str(source.schema_url))
        if isinstance(raw, str):
            if '```json' in raw:
                raw = raw.split('```json\n')[1].split('```')[0]
            schema = json.loads(raw)
        elif isinstance(raw, dict):
            schema = raw
        else:
            raise TypeError(f"Unexpected schema type from LLM: {type(raw)}")
        
        if "baseSelector" not in schema or "fields" not in schema:
            raise ValueError("Generated schema is missing 'baseSelector' or 'fields'")
        if not schema["baseSelector"] or not isinstance(schema["fields"], list):
             raise ValueError("Generated schema has invalid 'baseSelector' or 'fields'")

        log.info(f"Successfully generated schema for {source.name!r}")
        log.debug(f"Schema for {source.name!r}:\n{json.dumps(schema, indent=2)}")
        return schema
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        log.error(f"Failed to generate or parse a valid schema for {source.name}: {e}")
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred during schema generation for {source.name}: {e}")
        raise


def _generate_schema_from_llm(
    url: str,
    query=DEFAULT_QUERY
) -> dict:
    """Helper function to perform the actual LLM call to Gemini."""
    page = requests.get(url).text
    soup = BeautifulSoup(page, "lxml")
    html_snippet = soup.encode_contents().decode() if soup else page
    pruner = PruningContentFilter(threshold=0.5)
    filtered_chunks = pruner.filter_content(html_snippet)
    html_for_schema = "\n".join(filtered_chunks)

    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY environment variable not set. It is required for Gemini.")

    # CORRECTED: The provider name for Gemini should be 'gemini/*' not 'google/*'
    llm_cfg = LLMConfig(
        provider="gemini/gemini-1.5-flash-latest",
        api_token=api_key,
        temprature=0.0
    )
    
    schema = JsonCssExtractionStrategy.generate_schema(
        html=html_for_schema,
        schema_type="CSS",
        query=query,
        target_json_example=json.dumps([{
            "course_code": "BIOL 0280",
            "course_title": "Biochemistry",
            "course_description": "Lectures and recitation sections explore…"
        }], indent=2),
        llm_config=llm_cfg
    )
    
    return schema
