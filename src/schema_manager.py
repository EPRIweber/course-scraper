# src/schema_manager.py
import json
from pathlib import Path
from src.config import SourceConfig
from crawl4ai import JsonCssExtractionStrategy, LLMConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from bs4 import BeautifulSoup
import requests, os, json
from pathlib import Path


DEFAULT_QUERY="""
Generate a JSON schema (not the data!) using valid CSS selectors that will be used to select distinct course blocks from the given HTML.

Requirements:
- **Output must be valid JSON only**, following exactly the structure below.
- **Mandatory fields** (every course block will ALWAYS have these):
  - `course_title`
  - `course_description`
- **Optional field** (include only if it can be separated via their own CSS selector):
  - `course_code`

**Schema must be structured like this**:

{
  "name": "Course Block",
  "baseSelector": "<CSS selector, e.g. div.courseblock>",
  "fields": [
    { "name": "course_title",       "selector": "<CSS selector>", "type": "text" },
    { "name": "course_description", "selector": "<CSS selector>", "type": "text" }
    // course_code included if present and seperable
  ]
}
"""

def generate_schema_from_llm(
    url: str,
    query=DEFAULT_QUERY
) -> str:
    page = requests.get(url).text
    soup = BeautifulSoup(page, "lxml")
    html_snippet = soup.encode_contents().decode() if soup else page
    pruner = PruningContentFilter(threshold=0.5)
    filtered_chunks = pruner.filter_content(html_snippet)
    html_for_schema = "\n".join(filtered_chunks)

    llm_cfg = LLMConfig(
        provider="openai/gpt-4o-mini",
        api_token=os.getenv("OPENAI_API_KEY"),
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

def generate_schema(
    source: SourceConfig,
) -> dict:
    schema = generate_schema_from_llm(source.schema_url, query=source.query)
    return json.loads(schema)