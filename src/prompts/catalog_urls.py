from __future__ import annotations
from typing import List, Dict
from .base import PromptBase, register


@register("catalog_root")
class CatalogRootPrompt(PromptBase):
    def __init__(self, school: str, pages: List[Dict[str, str]]):
        self.school = school
        self.pages = pages

    def system(self) -> str:
        return (
            "You are an assistant that selects the correct course catalog root URL for a university. "
            "The root URL should be a browsable web catalog listing courses, not an archive or PDF. "
            'Choose the best candidate from the information provided and reply with JSON like {"root_url": "..."}.'
        )

    def user(self) -> str:
        parts = [f"School: {self.school}", "Candidate pages:"]
        for i, p in enumerate(self.pages, 1):
            parts.append(f"[{i}] {p['url']}\n{p['snippet']}")
        return "\n\n".join(parts)


@register("catalog_schema")
class CatalogSchemaPrompt(PromptBase):
    def __init__(self, school: str, root_url: str, pages: List[Dict[str, str]]):
        self.school = school
        self.root_url = root_url
        self.pages = pages

    def system(self) -> str:
        return (
            "You are an assistant that selects a representative course detail page from a course catalog. "
            "The URL returned should have the same structure as other course pages. "
            'Reply with JSON like {"schema_url": "..."}.'
        )

    def user(self) -> str:
        parts = [
            f"School: {self.school}",
            f"Catalog root: {self.root_url}",
            "Candidate pages:",
        ]
        for i, p in enumerate(self.pages, 1):
            parts.append(f"[{i}] {p['url']}\n{p['snippet']}")
        return "\n\n".join(parts)
