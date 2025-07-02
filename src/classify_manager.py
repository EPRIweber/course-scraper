# src/classify_manager.py
from llm_client import GemmaClient
from src.prompts.taxonomy import taxonomy_sys_prompt
import json

def classify_course(title, desc) -> tuple[str, int]:

    client = GemmaClient()
    # client.set_response_format({
    #   "type":"json_object",
    #   "json_schema":{
    #     "name":"CourseClassification",
    #     "schema":{
    #        "type":"object",
    #        "properties":{
    #            "related":{"type":"boolean"},
    #            "labels" :{"type":"array","items":{"type":"string"}}
    #        },
    #        "required":["related","labels"]
    #     },
    #     "strict":True
    #   }
    # })

    response = client.chat([
        {"role":"system","content":taxonomy_sys_prompt},
        {"role":"user",  "content":f"""## Title:
{title}

## Description:
{desc}
"""},
    ])
    usage: int = response.get("usage", {})
    content: str = response["choices"][0]["message"]["content"]
    return content, usage