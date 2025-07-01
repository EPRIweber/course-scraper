# src/prompts/schema.py
import json
from typing import Optional
from .base import PromptBase, register
from .defaults import SCHEMA_BUILDER

@register("find_repeating")
class FindRepeating(PromptBase):
    
    def __init__(
            self,
            *,
            html: str,
            required_fields: Optional[list[str]] = None,
            optional_fields: Optional[list[str]] = None,
            type: Optional[str] = "css",
            role: Optional[str] = "You specialize in generating JSON extraction schemas for web scraping.",
            repeating_block: Optional[str] = None,
            target_json_example: Optional[str] = None
    ):
        self.type = type.lower() if type.lower() in ["css", "xpath"] else "css"
        self.base_prompt = SCHEMA_BUILDER[type]
        self.html = html
        self.role = role

        self.block_description = f"Within the given HTML, first you must identify the baseSelector to select distinct {repeating_block} instances." if repeating_block else "First you must identify the baseSelector to select the target repeating block."
        self.fields_description = "The fields extracted for this schema **MUST** come from the field described below." if required_fields else ("You may use the fields provided below as examples for what to extract:" if (required_fields or optional_fields) else "It is up to you to decide the fields for extracting" )
        required_formatted = "\n".join(f" - {f}" for f in required_fields) if required_fields else None
        self.required_description = f"\n# The repeating block will **ALWAYS** have the required fields:\n{required_formatted}" if required_fields else None
        optional_formatted = "\n".join(f" - {f}" for f in optional_fields) if optional_fields else None
        self.optional_description = f"\n# The repeating block **MAY** have the optional fields:\n{optional_formatted}" if optional_fields else None
        self.json_description = f"# Example of target JSON object:\n```json\n{target_json_example}\n```" if target_json_example else None


    def system(self) -> str:
        return f"""You specialize in generating special JSON schemas for web scraping. This schema uses {self.type.upper()} selectors to present a repetitive pattern in crawled HTML, such as a product in a product list or a search result item in a list of search results. We use this JSON schema to pass to a language model along with the HTML content to extract structured data from the HTML. The language model uses the JSON schema to extract data from the HTML and retrieve values for fields in the JSON schema, following the schema.

Generating this HTML manually is not feasible, so you need to generate the JSON schema using the HTML content. The HTML copied from the crawled website is provided below, which we believe contains the repetitive pattern.

# Schema main keys:
- name: This is the name of the schema.
- baseSelector: This is the {self.type.upper()} selector that identifies the base element that contains all the repetitive patterns.
- baseFields: This is a list of fields that you extract from the base element itself.
- fields: This is a list of fields that you extract from the children of the base element. {{name, selector, type}} based on the type, you may have extra keys such as "attribute" when the type is "attribute".

# Extra Context:
- Example of target JSON object: This is a sample of the final JSON object that we hope to extract from the HTML using the schema you are generating.
- Extra Instructions: These additional instructions to provided to help you generate the schema for this specific scraping job.
- Query or explanation of target/goal data item: This is a description of what data we are trying to extract from the HTML. This explanation means we're not sure about the rigid schema of the structures we want, so we leave it to you to use your expertise to create the best and most comprehensive structures aimed at maximizing data extraction from this page. You must ensure that you do not pick up nuances that may exist on a particular page. The focus should be on the data we are extracting, and it must be valid, safe, and robust based on the given HTML.

{self.base_prompt}
"""

    def user(self) -> str:
        return f"""HTML to analyze:
```html
{self.html}
```



## Query/explanation of target data:
{self.role}
{self.block_description}
{self.fields_description}
{self.required_description}
{self.optional_description}



# Example of target JSON object:
{self.json_description}


IMPORTANT SELF-CHECK:
- **Selector reliability:** Ensure your schema remains reliable by avoiding selectors that appear to generate dynamically and are not dependable. You want a reliable schema, as it consistently returns the same data even after many page reloads.
- **Scoped matching:** Verify that all child fields of the baseSelector are actually contained inside of the base selector, ensuring that document.querySelectorAll(baseSelector + ' ' + selector) returns at least one element.
- **Strict output:** Return a JSON schema that follows the specified format precisely. Only output valid JSON schema, no explanatory text.
"""