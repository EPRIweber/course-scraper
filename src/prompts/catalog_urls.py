# src/prompts/catalog_urls.py
from __future__ import annotations
from typing import List, Dict
from .base import PromptBase, register


@register("catalog_root")
class CatalogRootPrompt(PromptBase):
    def __init__(self, school: str, pages: List[Dict[str, str]]):
        self.school = school
        self.pages = pages

    def system(self) -> str:
        return ("""
You are an assistant that selects the correct course catalog root URL for a school. 
The root URL should be a browsable and up-to-date web catalog listing courses, not an archive or PDF.
Choose the best candidate from the information provided that is the best root page for scraping courses.

General guide for identifying best root URL:
 - Select the “closest” endpoint (deepest common parent) from which you can click directly to all course listings (i.e. the page from which you can reach every other listing in the fewest clicks).
 - If no single deep endpoint links to every listing (e.g. separate undergraduate/graduate listing), instead pick the more shallow page that still links to the largest number of individual course pages, even if it means an extra click or two for some listings. (may require looking at links within each page to find root)
 - Common root urls are pages that contain paths such as '/courses', '/content', '/coursesaz', '/course-descriptions', etc.
 - If multiple catalog years are listed, select the newest URL which meets the above criteria for a root URL.

## **IMPORTANT**:
 - The provided pages are ordered based on likelihood for being the correct root URL. If there are multiple potential candidates, you should choose the URL listed FIRST.
 - Be sure to select the root URL for course catalog description information, do NOT get confused by additional links to degree options, department information, online only versions of the school, or other data provided. It is ok if other information is accessible from the selected link, but the required course information MUST be accessible.
 - Reply **only** with JSON: 
    {
        "root_url": "<url_link>"
    }"""
        )

    def user(self) -> str:
        parts = [f"# School: {self.school}", "## Candidate pages:"]
        for i, p in enumerate(self.pages, 1):
            parts.append(f"### [{i}] {p['url']}\n{p['snippet']}")
        return "\n\n".join(parts)


@register("catalog_schema")
class CatalogSchemaPrompt(PromptBase):
    def __init__(self, school: str, root_url: str, pages: List[Dict[str, str]]):
        self.school   = school
        self.root_url = root_url
        self.pages    = pages

    def system(self) -> str:
        return ("""
You are a schema‐finder assistant. 
Given a known course catalog root page, select one URL that is a **representative detail page**. Thid page should:
    - Contain at least one course entry with both title and description
    - If the title or description appears to be cut off (such as ending in ...), do not return the url and instead find the page with the complete course.
    - Be representative of all course pages if there are multiple possible 'schema url'
Reply **only** with JSON:\n"
`{"schema_url": "https://..."}`"""
        )

    def user(self) -> str:
        parts = [
            f"# School: {self.school}",
            f"## Catalog root: {self.root_url}",
            "## Candidate detail pages:"
        ]
        for i, p in enumerate(self.pages, 1):
            parts.append(f"### [{i}] {p['url']}\nSnippet:\n{p['snippet']}\n")
        parts.append(
            "\nSELF‐CHECK: Confirm that the chosen URL’s text snippet shows a course title and "
            "description in distinct elements (e.g. `Course Title: ...` and `Description: ...`), "
            "and that this pattern appears on every course page under the root."
        )
        return "\n\n".join(parts)