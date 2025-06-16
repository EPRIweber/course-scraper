# src/prompts/find_repeating.py
from .defaults import SCHEMA_BUILDER
from .base import PromptBase
from .prompt_registry import register

@register("find_repeating")
class FindRepeating(PromptBase):
    """
    FindRepeating is a class designed to intitialize and maintain LLM prompts for generating JSON webscraping schemas.

    Works best for data that is:
       -  Repetitive in nature, such as product listings or search results.
       -  Data separated by distinct HTML selectors.
       -  Requires structured data extraction using CSS or XPATH selectors.
       -  Scraping multiple pages with similar structures.


    # Included Fields
    
    ## Required:
      -   `selector_type`: The type of selector to use, either "css" or "xpath". Defaults to "css".
      -   `target_html`: The HTML content that will be analyzed to generate the JSON schema.

    ## Optional:
      -   `role`: The role of the agent, which specializes in generating JSON schemas for web scraping. Defaults to a generic role.
      -   `explicit_fields`: Whether to instruct the LLM to only used field provided. Defaults to True
      -   `repeating_block`: The repeating block of HTML that contains the data to be extracted. Defaults to None.
      -   `required_fields`: A list of required fields that must be present in the generated JSON schema. Defaults to an empty list.
      -   `optional_fields`: A list of optional fields that may be present in the generated JSON schema. Defaults to an empty list.
      -   `target_json_example`: A sample of the final JSON object that we hope to
    """
    
    # Prompt template for generating JSON schema
    template = """You specialize in generating special JSON schemas for web scraping. This schema uses CSS or XPATH selectors to present a repetitive pattern in crawled HTML, such as a product in a product list or a search result item in a list of search results. We use this JSON schema to pass to a language model along with the HTML content to extract structured data from the HTML. The language model uses the JSON schema to extract data from the HTML and retrieve values for fields in the JSON schema, following the schema.

Generating this HTML manually is not feasible, so you need to generate the JSON schema using the HTML content. The HTML copied from the crawled website is provided below, which we believe contains the repetitive pattern.

# Schema main keys:
- name: This is the name of the schema.
- baseSelector: This is the CSS or XPATH selector that identifies the base element that contains all the repetitive patterns.
- baseFields: This is a list of fields that you extract from the base element itself.
- fields: This is a list of fields that you extract from the children of the base element. {{name, selector, type}} based on the type, you may have extra keys such as "attribute" when the type is "attribute".

# Extra Context:
- Example of target JSON object: This is a sample of the final JSON object that we hope to extract from the HTML using the schema you are generating.
- Extra Instructions: These additional instructions to provided to help you generate the schema for this specific scraping job.
- Query or explanation of target/goal data item: This is a description of what data we are trying to extract from the HTML. This explanation means we're not sure about the rigid schema of the structures we want, so we leave it to you to use your expertise to create the best and most comprehensive structures aimed at maximizing data extraction from this page. You must ensure that you do not pick up nuances that may exist on a particular page. The focus should be on the data we are extracting, and it must be valid, safe, and robust based on the given HTML.

# What are the instructions and details for generating the schema?
{prompt_template}



HTML to analyze:
```html
{target_html}
```



## Query or explanation of target/goal data item:
{role}
{block_description}
{fields_description}
{required_description}
{optional_description}



# Example of target JSON object:
{target_json_example}


IMPORTANT: Ensure your schema remains reliable by avoiding selectors that appear to generate dynamically and are not dependable. You want a reliable schema, as it consistently returns the same data even after many page reloads.
Analyze the HTML and generate a JSON schema that follows the specified format. Only output valid JSON schema, nothing else.
"""

    selector_type = "css"
    role = None
    explicit_fields = True
    repeating_block = None
    required_fields = []
    optional_fields = []
    target_html = None
    target_json_example = None

    def set_type(self, selector_type: str):
        """
        Initialize the FindRepeating class with a specific selector type that will be returned.
        
        - Default: "css".
        - Options: "css" or "xpath".
        """
        if selector_type not in ["css", "xpath"]:
            raise ValueError("selector_type must be either 'css' or 'xpath' (defaults to 'css')")
        self.selector_type = selector_type
    
    def set_role(self, role: str):
        """
        Set the role for the FindRepeating agent.
        
        - Default: "You specialize in generating JSON schemas for web scraping."
        """
        role = role.strip()
        self.role = role
    
    def set_repeating_block(self, repeating_block: str):
        """
        Set the repeating block for the FindRepeating class.
        
        - Default: AI will be instructed to to determine the baseSelector if not specified.
        """
        repeating_block = repeating_block.strip()
        self.repeating_block = repeating_block
    
    def set_required_fields(self, required_fields: list[str]):
        """
        Set the required fields for the FindRepeating class.

        - Default: AI will be instructed to determine fields if not specified..
        - Ex: ["course_title", "course_description"]
        """
        if not isinstance(required_fields, list):
            raise TypeError("required_fields must be a list of strings")
        self.required_fields = [field.strip() for field in required_fields if field.strip()]
    
    def set_optional_fields(self, optional_fields: list[str]):
        """
        Set the optional fields for the FindRepeating class.
        These fields may be present in the generated JSON schema, but only if present and separable.
        """
        if not isinstance(optional_fields, list):
            raise TypeError("optional_fields must be a list of strings")
        self.optional_fields = [field.strip() for field in optional_fields if field.strip()]
    
    def set_target_html(self, target_html: str):
        """
        Set the target HTML for the FindRepeating class.
        This is the HTML content that will be analyzed to generate the JSON schema.
        """
        target_html = target_html.strip()
        if not target_html:
            raise ValueError("target_html cannot be empty")
        self.target_html = target_html

    def set_target_json_example(self, target_json_example: str):
        """
        Set the target JSON example for the FindRepeating class.
        This is a sample of the final JSON object that we hope to extract from the HTML using the schema.
        """
        target_json_example = target_json_example.strip()
        if not target_json_example:
            raise ValueError("target_json_example cannot be empty")
        self.target_json_example = target_json_example
    
    def build_prompt(self) -> str:
        """
        Build the prompt for the FindRepeating class.

        Options:
        - `selector_type`
        - `target_html`
        - `role`
        - `repeating_block`
        - `required_fields`
        - `optional_fields`
        - `target_json_example`
        """
        if not self.target_html:
            raise ValueError("target_html must be set before building the prompt")
        
        prompt_template = SCHEMA_BUILDER[self.selector_type]
        target_html = self.target_html
        role = self.role or "You specialize in generating JSON schemas for web scraping."
        block_description = f"Within the given HTML, first you must identify the baseSelector to select distinct {self.repeating_block} instances." if self.repeating_block else "First you must identify the baseSelector to select the target repeating block."
        fields_description = "The fields extracted for this schema **MUST** come from the field examples provided." if self.required_fields else ("You may use the fields provided below as examples for what to extract:" if (self.required_fields or self.optional_fields) else "It is up to you to decide the fields for extracting" )
        required_fields = "\n".join(f" - {f}" for f in self.required_fields) if self.required_fields else ""
        required_description = f"# The repeating block will **ALWAYS** have the required fields:\n{required_fields}" if self.required_fields else ""
        optional_fields = "\n".join(f" - {f}" for f in self.optional_fields) if self.optional_fields else ""
        optional_description = f"# The repeating block **MAY** have the optional fields:\n{optional_fields}" if self.optional_fields else ""
        target_json_example = f"# Example of target JSON object:\n```json\n{self.target_json_example}\n```" if self.target_json_example else ""

        return self.render(
            template=self.template,
            prompt_template=prompt_template,
            target_html=target_html,
            role=role,
            block_description=block_description,
            fields_description=fields_description,
            required_description=required_description,
            optional_description=optional_description,
            target_json_example=target_json_example
        )
