# src/classify_manager.py
from llm_client import GemmaClient
from src.prompts.taxonomy import taxonomy_sys_prompt
import json


def classify_courses(courses: list[tuple[str, str, str]]) -> list[tuple[str, list[str]]]:
    """
    # Inputs
    ## Courses:
    ### List of [
    - course_id: str,
    - course_title: str,
    - course_description
    ]
    
    # Output
    ## Classified Courses
    ### List of [
    - course_id: str,
    - classifications: list[str]
    ]
    """

    pass

def _classify_course(title: str, desc:str) -> tuple[str, int]:

    client = GemmaClient()

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