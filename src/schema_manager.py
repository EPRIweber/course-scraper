# src/schema_manager.py

import requests, json, logging

from pydantic import HttpUrl
from typing import List
from src.config import SourceConfig, ValidationCheck
from crawl4ai.content_filter_strategy import PruningContentFilter
from bs4 import BeautifulSoup

from src.llm_wrapper import LlamaModel, GemmaModel
from src.prompts.find_repeating import FindRepeating
from src.scraper import scrape_urls

GEMMA="google/gemma-3-27b-it"
GEMMA_URL="http://epr-ai-lno-p01.epri.com:8000"
LLAMA="meta/llama-3.2-90b-vision-instruct"
LLAMA_URL="http://epr-ai-lno-p01.epri.com:8002"

async def generate_schema(
    source: SourceConfig,
) -> dict:
    log = logging.getLogger(__name__)
    schema, usage = await _generate_schema_from_llm(url=source.schema_url)
    log.info(f"Generated schema for {source.name!r}:\n{schema}")
    return schema, usage

async def _generate_schema_from_llm(
    url: HttpUrl,
) -> dict:
    """Helper function to perform the actual LLM call to Gemini."""
    page = requests.get(url).text
    soup = BeautifulSoup(page, "lxml")
    html_snippet = soup.encode_contents().decode() if soup else page
    pruner = PruningContentFilter(threshold=0.5)
    filtered_chunks = pruner.filter_content(html_snippet)
    html_for_schema = "\n".join(filtered_chunks)
    log = logging.getLogger(__name__)
    log.info(f"generating schema using html with {len(html_for_schema)} characters")
    
    course_prompt: FindRepeating = FindRepeating()
    course_prompt.set_role("You specialize in exacting structured course data from course catalog websites.")
    course_prompt.set_repeating_block("course_block")
    course_prompt.set_required_fields(["course_title", "course_description"])
    course_prompt.set_optional_fields(["course_code"])
    course_prompt.explicit_fields = True
    course_prompt.set_target_html(html_for_schema)
    course_prompt.set_target_json_example(
        json.dumps([{
            "course_title": "Biochemistry",
            "course_description": "Lectures and recitation sections explore the structure and function of biological molecules, including proteins, nucleic acids, carbohydrates, and lipids. Topics include enzyme kinetics, metabolic pathways, and the molecular basis of genetic information.",
            "course_code": "BIOL 0280"
        }], indent=2)
    )
    sys_prompt = course_prompt.build_sys_prompt()
    user_prompt = course_prompt.build_user_prompt()

    # TODO: Add to classifier sys prompt
    # The user will provide the title and description for the course

    llm = GemmaModel(api_url=GEMMA_URL)
    system_message = { "role": "system", "content": sys_prompt}
    user_message = {"role": "user", "content": user_prompt}

    response = llm.chat_completion(
        model=GEMMA,
        messages=[system_message, user_message],
        max_tokens=30000,
        temperature=0.0,
        response_format={
            "type": "json_object",
            "json_schema": {
                "name": "CourseExtractionSchema",
                "description": "Schema for extracting structured course data from course catalog websites.",
                "schema": {
                    "type": "object",
                    "properties": {
                        "name":          {"type": "string"},
                        "baseSelector":  {"type": "string"},
                        "fields": {
                            "type":     "array",
                            "items":    {"type": "object"}
                        }
                    },
                    "required": ["name", "baseSelector", "fields"]
                },
                "strict": True
            }
        }
    )

    content = response["choices"][0]["message"]["content"]
    obj = json.loads(content)
    if isinstance(obj, list):
        if len(obj) == 1:
            obj = obj[0]
        else:
            raise ValueError("LLM returned an array; expected a single schema object")

    usage   = response.get("usage", {})

    try:
        return obj, usage
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse schema JSON:\n{content}") from e

async def validate_schema(
    schema: dict,
    source: SourceConfig,
    *,
    required_fields: List[str] | None = None
) -> ValidationCheck:
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

    required_fields = required_fields or ["course_title", "course_description"]
    fields_missing: list[str] = []
    errors: list[str] = []

    try:
        # Scrape just the schema_url page
        records, _, _, json_errors = await scrape_urls(
            urls=[str(source.schema_url)],
            schema=schema,
            source=source
        )

        # surface JSON decode errors, if any
        if json_errors:
            errors.extend(json_errors)

        if not records:
            errors.append("No records extracted from the test page.")
        else:
            # check that each required field appears at least once
            for field in required_fields:
                if not any(field in rec and rec[field] for rec in records):
                    fields_missing.append(field)

    except Exception as exc:
        log.exception("Schema validation failed")
        errors.append(str(exc))

    valid = not errors and not fields_missing
    return ValidationCheck(
        valid=valid,
        fields_missing=fields_missing,
        errors=errors
    )
