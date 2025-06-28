# src/prompts/classifier.py
from .base import PromptBase, register

@register("classify_course")
class ClassifyCoursePrompt(PromptBase):
    def __init__(self, *, title: str, desc: str):
        self.title = title
        self.desc  = desc

    def system(self) -> str:
        return (
            "You are an expert in hydrogen-industry topics. "
            "Given a course title and description, decide if it is related to hydrogen "
            "and assign labels from this set: [‘electrolysis’, ‘fuel_cells’, …].\n"
            "Output JSON: { related: bool, labels: [string] }"
        )

    def user(self) -> str:
        return f"""
Course Title: {self.title}

Description:
{self.desc}

Return exactly:
{{
  "related": true|false,
  "labels": [ ... ]
}}
"""