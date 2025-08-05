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

        return f"""
You are an assistant designed to identify the correct course catalog root URL for the school {self.school}. 
The root URL should be a browsable and up-to-date web catalog listing courses, not an archive or PDF.
The full description of these courses may be on the current page, or they may require an additional clicks to preview, but you MUST be able to see courses listed on the root page.

General guide for identifying best root URL:
 - If courses apprear on multiple pages, select the “closest” endpoint (deepest common parent) from which you can click directly to all course listings (i.e. the page from which you can reach every other listing in the fewest clicks).
 - Common root urls have pages such as '/courses', '/content', '/coursesaz', '/course-descriptions', etc.
 - If multiple catalog years are listed, select the newest URL which meets the above criteria for a root URL.

## **IMPORTANT**:
 - The provided pages are ordered based on likelihood for being the correct root URL.
 - Be sure to select the root URLs containing either course description information or direct links to course description information (e aware that some links may be pruned from the HTML snippet provided). The required course information **MUST** be directly accessible from the root URL.
 - **DO NOT GET CONFUSED** and return a page that may contain information about courses, but is not the actual course catalog with course descriptions. Here are some examples of this type of false positive:
     - Degree Options
     - Department Information
     - Course Schedules
     - General Bulletin
 - Avoid websites which look like the school's site, but are an online only versions of the school.""" + """

### Reply **only** with JSON in exactly one of these two forms:

```json
{"root_url": "<url_link>"}
````

– when you have positively identified a valid root URL with course descriptions,

**OR**

```json
{"root_url": null}
```

– if no valid root URL can be determined.

Do not include any other keys or commentary.
"""

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
Given a known course catalog root page, select one URL that is a **representative detail page**. This page should:
    - Contain at least one course entry with both title and description
    - If the title or description appears to be cut off (such as ending in ...), do not return the url and instead find the page with the complete course.
    - Be representative of all course pages if there are multiple possible 'schema url'

## **IMPORTANT**:
 - You should prioritize pages that have only one course on the page. Often pages will have a table of incomplete course descriptions with links to the individual course. You should choose the individual course link in this case.

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