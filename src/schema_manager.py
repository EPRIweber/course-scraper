# src/schema_manager.py
import json
import logging
import os
from pathlib import Path

from pydantic import HttpUrl
from src.config import SourceConfig
from crawl4ai import JsonCssExtractionStrategy, LLMConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from bs4 import BeautifulSoup
import requests, os, json
from pathlib import Path

from src.llm_wrapper import LlamaModel
from src.prompts.find_repeating import FindRepeating

GEMMA="google/gemma-3-27b-it"
LLAMA="meta/llama-3.2-90b-vision-instruct"
LLAMA_URL="http://epr-ai-lno-p01.epri.com:8002"


async def generate_schema(
    source: SourceConfig,
) -> dict:
    log = logging.getLogger(__name__)
    schema, usage = await _generate_schema_from_llm(url=source.schema_url)
    log.info(f"Generated schema for {source.name!r}:\n{schema}")
    return schema, usage

def _generate_schema_from_llm(
    url: HttpUrl,
) -> dict:
    """Helper function to perform the actual LLM call to Gemini."""
    page = requests.get(url).text
    soup = BeautifulSoup(page, "lxml")
    html_snippet = soup.encode_contents().decode() if soup else page
    pruner = PruningContentFilter(threshold=0.5)
    filtered_chunks = pruner.filter_content(html_snippet)
    html_for_schema = "\n".join(filtered_chunks)
    # log characters in the html before sending to LLM
    
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

    llm = LlamaModel(api_url=LLAMA_URL)
    system_message = { "role": "system", "content": sys_prompt}
    user_message = {"role": "user", "content": user_prompt}

    response = llm.chat_completion(
        model=LLAMA,
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
    usage   = response.get("usage", {})

    try:
        return json.loads(content), usage
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse schema JSON:\n{content}") from e

