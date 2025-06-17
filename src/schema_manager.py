# src/schema_manager.py
import json
import logging
from pathlib import Path

from pydantic import HttpUrl
from src.config import SourceConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from bs4 import BeautifulSoup
import requests, os, json
from pathlib import Path

from src.llm_wrapper import LlamaModel
from src.prompts.find_repeating import FindRepeating

GEMMA="google/gemma-3-27b-it"
LLAMA="meta/llama-3.2-90b-vision-instruct"
EPRI_URL="http://epr-ai-lno-p01.epri.com:8002"

async def generate_schema(
    source: SourceConfig,
) -> dict:
    schema, usage = await _generate_schema_from_llm(url=source.schema_url)
    return schema, usage

async def _generate_schema_from_llm(
    url: HttpUrl
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
    sys_prompt = course_prompt.build_sys_prompt()
    user_prompt = course_prompt.build_user_prompt()

    llm = LlamaModel(api_url=EPRI_URL)
    system_message = { "role": "system", "content": sys_prompt}
    user_message = {"role": "user", "content": user_prompt}

    response = llm.chat_completion(
        model=LLAMA,
        messages=[system_message, user_message],
        max_tokens=131072,
        temperature=0.0,
        response_format={
            "type": "json_object",
            "json_schema": {
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
    )

    content = response["choices"][0]["message"]["content"]
    usage   = response.get("usage", {})

    try:
        return json.loads(content), usage
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse schema JSON:\n{content}") from e

