# src/classify_manager.py
from src.llm_client import GemmaModel
from src.prompts.taxonomy import taxonomy_sys_prompt

async def classify_courses(courses: list[tuple[str, str, str]]) -> tuple[list[tuple[str, list[str]]], int]:
    """
    Classify a batch of courses via LLM into taxonomy labels.

    Parameters
    ----------
    courses : list of (course_id, title, description)

    Returns
    -------
    list of (course_id, list of classification labels)
    """
    results: list[tuple[str, list[str]]] = []
    total_usage = 0
    for course_id, title, desc in courses:
        try:
            labels_str, usage = _classify_course(title, desc)
            # split on commas (or newlines) for multiple labels
            labels = [lbl.strip() for lbl in labels_str.replace("\n", ",").split(',') if lbl.strip()]
            results.append((course_id, labels))
            total_usage += usage
        except Exception as e:
            # on failure, record empty list or a marker
            results.append((course_id, []))
    return results, total_usage

async def _classify_course(title: str, desc:str) -> tuple[str, int]:

    client = GemmaModel()

    response = client.chat([
        {"role":"system","content": taxonomy_sys_prompt},
        {"role":"user", "content": f"## Title:\n{title}\n\n## Description:\n{desc}"},
    ])
    usage: int = response.get("usage", {})
    content: str = response["choices"][0]["message"]["content"]
    return content, usage