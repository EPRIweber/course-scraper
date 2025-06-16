# src/schema_manager.py
import json
import logging
from pathlib import Path

from pydantic import HttpUrl
from src.config import SourceConfig
from crawl4ai import JsonCssExtractionStrategy, LLMConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from bs4 import BeautifulSoup
import requests, os, json
from pathlib import Path

from src.prompts.find_repeating import FindRepeating


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
    { "name": "course_title",       "selector": "<CSS selector>", "type": "<text or attribute>" },
    { "name": "course_description", "selector": "<CSS selector>", "type": "<text or attribute>" },
    { "name": "course_code",        "selector": "<CSS selector>", "type": "<text or attribute>" }
  ]
}
"""

GEMMA="google/gemma-3-27b-it"
LLAMA="meta/llama-3.2-90b-vision-instruct"
URL="http://epr-ai-lno-p01.epri.com:8000/v1/chat/completions"

async def generate_schema(
    source: SourceConfig,
) -> dict:
    log = logging.getLogger(__name__)
    raw = await _generate_schema_from_llm(url=source.schema_url)
    return raw
    if isinstance(raw, str):
        schema = json.loads(raw)
    elif isinstance(raw, dict):
        schema = raw
    else:
        raise TypeError(f"Unexpected schema type: {type(raw)}")
    log.info(f"Generated schema for {source.name!r}:\n{schema}")
    return schema

async def _generate_schema_from_llm(
    url: HttpUrl,
    query=DEFAULT_QUERY
) -> dict:
    page = requests.get(str(url)).text
    soup = BeautifulSoup(page, "lxml")
    html_snippet = soup.encode_contents().decode() if soup else page
    pruner = PruningContentFilter(threshold=0.3)
    filtered_chunks = pruner.filter_content(html_snippet)
    html_for_schema = "\n".join(filtered_chunks)


    course_prompt: FindRepeating = FindRepeating()
    course_prompt.set_role("You specialize in exacting structured course data from course catalog websites.")
    course_prompt.set_repeating_block("course block")
    course_prompt.set_required_fields(["course_title", "course_description"])
    course_prompt.set_optional_fields(["course_code"])
    course_prompt.explicit_fields = True
    course_prompt.set_target_html(html_for_schema)
    course_prompt.set_target_json_example(
        json.dumps([{
            "course_code": "BIOL 0280",
            "course_title": "Biochemistry",
            "course_description": "Lectures and recitation sections explore the structure and function of biological molecules, including proteins, nucleic acids, carbohydrates, and lipids. Topics include enzyme kinetics, metabolic pathways, and the molecular basis of genetic information."
        }], indent=2)
    )
    return course_prompt.build_prompt()
    

    # llm_cfg = LLMConfig(
    #     provider="openai/gpt-4o-mini",
    #     api_token=os.getenv("OPENAI_API_KEY"),
    #     temprature=0.0
    # )
    # llm_cfg = LLMConfig(`
    #     # provider="meta/llama-3.2-90b-vision-instruct",
    #     provider="google/gemma-3-27b-it",
    #     base_url="http://epr-ai-lno-p01.epri.com:8000/v1/chat/completions",
    #     api_token="null"
    # )`
    
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
