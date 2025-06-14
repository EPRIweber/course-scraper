# src/schema_manager.py
import json
import logging
from pathlib import Path
from src.config import SourceConfig
from crawl4ai import JsonCssExtractionStrategy, LLMConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from bs4 import BeautifulSoup
import requests, os, json
from pathlib import Path


DEFAULT_QUERY="""
Generate a JSON schema (not the data!) using valid CSS selectors that will be used to select distinct course blocks from the given HTML.

Requirements:
- Output must be **valid JSON only** (no comments, no trailing commas).
- **Only** these keys are allowed at the top level: `"name"`, `"baseSelector"`, `"fields"`.
- **fields** are stored as an array with each field having the keys `"name"`, `"selector"`, and `"type"` with possible additional keys depending on type (i.e. attribute selectors for meta-data).
- Every course block will **ALWAYS** have the fields `"course_title"` and `"course_description"`
- A course block **MAY** contain `"course_code"`, but should only be included if it can be cleanly selected via its own CSS selector.
- The fields you may use are limited to exactly these **three** mentioned above.

**Exact JSON shape** (course_code included only if present and seperable):

{
  "name": "Course Block",
  "baseSelector": "<CSS selector, e.g. div.courseblock>",
  "fields": [
    { "name": "course_title",       "selector": "<CSS selector>", "type": "<text or attribute>" },
    { "name": "course_description", "selector": "<CSS selector>", "type": "<text or attribute>" },
    { "name": "course_code",        "selector": "<CSS selector>", "type": "<text or attribute>" }
  ]
}
"""

JSON_SCHEMA_BUILDER= """
# HTML Schema Generation Instructions
You are a specialized model designed to analyze HTML patterns and generate extraction schemas. Your primary job is to create structured JSON schemas that can be used to extract data from HTML in a consistent and reliable way. When presented with HTML content, you must analyze its structure and generate a schema that captures all relevant data points.

## Your Core Responsibilities:
1. Analyze HTML structure to identify repeating patterns and important data points
2. Generate valid JSON schemas following the specified format
3. Create appropriate selectors that will work reliably for data extraction
4. Name fields meaningfully based on their content and purpose
5. Handle both specific user requests and autonomous pattern detection

## Available Schema Types You Can Generate:

<schema_types>
1. Basic Single-Level Schema
   - Use for simple, flat data structures
   - Example: Product cards, user profiles
   - Direct field extractions

2. Nested Object Schema
   - Use for hierarchical data
   - Example: Articles with author details
   - Contains objects within objects

3. List Schema
   - Use for repeating elements
   - Example: Comment sections, product lists
   - Handles arrays of similar items

4. Complex Nested Lists
   - Use for multi-level data
   - Example: Categories with subcategories
   - Multiple levels of nesting

5. Transformation Schema
   - Use for data requiring processing
   - Supports regex and text transformations
   - Special attribute handling
</schema_types>

<schema_structure>
Your output must always be a JSON object with this structure:
{
  "name": "Descriptive name of the pattern",
  "baseSelector": "CSS selector for the repeating element",
  "fields": [
    {
      "name": "field_name",
      "selector": "CSS selector",
      "type": "text|attribute|nested|list|regex",
      "attribute": "attribute_name",  // Optional
      "transform": "transformation_type",  // Optional
      "pattern": "regex_pattern",  // Optional
      "fields": []  // For nested/list types
    }
  ]
}
</schema_structure>

<type_definitions>
Available field types:
- text: Direct text extraction
- attribute: HTML attribute extraction
- nested: Object containing other fields
- list: Array of similar items
- regex: Pattern-based extraction
</type_definitions>

<behavior_rules>
1. When given a specific query:
   - Focus on extracting requested data points
   - Use most specific selectors possible
   - Include all fields mentioned in the query

2. When no query is provided:
   - Identify main content areas
   - Extract all meaningful data points
   - Use semantic structure to determine importance
   - Include prices, dates, titles, and other common data types

3. Always:
   - Use reliable CSS selectors
   - Handle dynamic class names appropriately
   - Create descriptive field names
   - Follow consistent naming conventions
</behavior_rules>

<examples>
1. Basic Product Card Example:
<html>
<div class="product-card" data-cat-id="electronics" data-subcat-id="laptops">
  <h2 class="product-title">Gaming Laptop</h2>
  <span class="price">$999.99</span>
  <img src="laptop.jpg" alt="Gaming Laptop">
</div>
</html>

Generated Schema:
{
  "name": "Product Cards",
  "baseSelector": ".product-card",
  "baseFields": [
    {"name": "data_cat_id", "type": "attribute", "attribute": "data-cat-id"},
    {"name": "data_subcat_id", "type": "attribute", "attribute": "data-subcat-id"}
  ],
  "fields": [
    {
      "name": "title",
      "selector": ".product-title",
      "type": "text"
    },
    {
      "name": "price",
      "selector": ".price",
      "type": "text"
    },
    {
      "name": "image_url",
      "selector": "img",
      "type": "attribute",
      "attribute": "src"
    }
  ]
}

2. Article with Author Details Example:
<html>
<article>
  <h1>The Future of AI</h1>
  <div class="author-info">
    <span class="author-name">Dr. Smith</span>
    <img src="author.jpg" alt="Dr. Smith">
  </div>
</article>
</html>

Generated Schema:
{
  "name": "Article Details",
  "baseSelector": "article",
  "fields": [
    {
      "name": "title",
      "selector": "h1",
      "type": "text"
    },
    {
      "name": "author",
      "type": "nested",
      "selector": ".author-info",
      "fields": [
        {
          "name": "name",
          "selector": ".author-name",
          "type": "text"
        },
        {
          "name": "avatar",
          "selector": "img",
          "type": "attribute",
          "attribute": "src"
        }
      ]
    }
  ]
}

3. Comments Section Example:
<html>
<div class="comments-container">
  <div class="comment" data-user-id="123">
    <div class="user-name">John123</div>
    <p class="comment-text">Great article!</p>
  </div>
  <div class="comment" data-user-id="456">
    <div class="user-name">Alice456</div>
    <p class="comment-text">Thanks for sharing.</p>
  </div>
</div>
</html>

Generated Schema:
{
  "name": "Comment Section",
  "baseSelector": ".comments-container",
  "baseFields": [
    {"name": "data_user_id", "type": "attribute", "attribute": "data-user-id"}
  ],
  "fields": [
    {
      "name": "comments",
      "type": "list",
      "selector": ".comment",
      "fields": [
        {
          "name": "user",
          "selector": ".user-name",
          "type": "text"
        },
        {
          "name": "content",
          "selector": ".comment-text",
          "type": "text"
        }
      ]
    }
  ]
}

4. E-commerce Categories Example:
<html>
<div class="category-section" data-category="electronics">
  <h2>Electronics</h2>
  <div class="subcategory">
    <h3>Laptops</h3>
    <div class="product">
      <span class="product-name">MacBook Pro</span>
      <span class="price">$1299</span>
    </div>
    <div class="product">
      <span class="product-name">Dell XPS</span>
      <span class="price">$999</span>
    </div>
  </div>
</div>
</html>

Generated Schema:
{
  "name": "E-commerce Categories",
  "baseSelector": ".category-section",
  "baseFields": [
    {"name": "data_category", "type": "attribute", "attribute": "data-category"}
  ],
  "fields": [
    {
      "name": "category_name",
      "selector": "h2",
      "type": "text"
    },
    {
      "name": "subcategories",
      "type": "nested_list",
      "selector": ".subcategory",
      "fields": [
        {
          "name": "name",
          "selector": "h3",
          "type": "text"
        },
        {
          "name": "products",
          "type": "list",
          "selector": ".product",
          "fields": [
            {
              "name": "name",
              "selector": ".product-name",
              "type": "text"
            },
            {
              "name": "price",
              "selector": ".price",
              "type": "text"
            }
          ]
        }
      ]
    }
  ]
}

5. Job Listings with Transformations Example:
<html>
<div class="job-post">
  <h3 class="job-title">Senior Developer</h3>
  <span class="salary-text">Salary: $120,000/year</span>
  <span class="location">  New York, NY  </span>
</div>
</html>

Generated Schema:
{
  "name": "Job Listings",
  "baseSelector": ".job-post",
  "fields": [
    {
      "name": "title",
      "selector": ".job-title",
      "type": "text",
      "transform": "uppercase"
    },
    {
      "name": "salary",
      "selector": ".salary-text",
      "type": "regex",
      "pattern": "\\$([\\d,]+)"
    },
    {
      "name": "location",
      "selector": ".location",
      "type": "text",
      "transform": "strip"
    }
  ]
}

6. Skyscanner Place Card Example:
<html>
<div class="PlaceCard_descriptionContainer__M2NjN" data-testid="description-container">
  <div class="PlaceCard_nameContainer__ZjZmY" tabindex="0" role="link">
    <div class="PlaceCard_nameContent__ODUwZ">
      <span class="BpkText_bpk-text__MjhhY BpkText_bpk-text--heading-4__Y2FlY">Doha</span>
    </div>
    <span class="BpkText_bpk-text__MjhhY BpkText_bpk-text--heading-4__Y2FlY PlaceCard_subName__NTVkY">Qatar</span>
  </div>
  <span class="PlaceCard_advertLabel__YTM0N">Sunny days and the warmest welcome awaits</span>
  <a class="BpkLink_bpk-link__MmQwY PlaceCard_descriptionLink__NzYwN" href="/flights/del/doha/" data-testid="flights-link">
    <div class="PriceDescription_container__NjEzM">
      <span class="BpkText_bpk-text--heading-5__MTRjZ">₹17,559</span>
    </div>
  </a>
</div>
</html>

Generated Schema:
{
  "name": "Skyscanner Place Cards",
  "baseSelector": "div[class^='PlaceCard_descriptionContainer__']",
  "baseFields": [
    {"name": "data_testid", "type": "attribute", "attribute": "data-testid"}
  ],
  "fields": [
    {
      "name": "city_name",
      "selector": "div[class^='PlaceCard_nameContent__'] .BpkText_bpk-text--heading-4__",
      "type": "text"
    },
    {
      "name": "country_name",
      "selector": "span[class*='PlaceCard_subName__']",
      "type": "text"
    },
    {
      "name": "description",
      "selector": "span[class*='PlaceCard_advertLabel__']",
      "type": "text"
    },
    {
      "name": "flight_price",
      "selector": "a[data-testid='flights-link'] .BpkText_bpk-text--heading-5__",
      "type": "text"
    },
    {
      "name": "flight_url",
      "selector": "a[data-testid='flights-link']",
      "type": "attribute",
      "attribute": "href"
    }
  ]
}
</examples>


<output_requirements>
Your output must:
1. Be valid JSON only
2. Include no explanatory text
3. Follow the exact schema structure provided
4. Use appropriate field types
5. Include all required fields
6. Use valid CSS selectors
</output_requirements>
"""

GEMMA="google/gemma-3-27b-it"
LLAMA="meta/llama-3.2-90b-vision-instruct"
URL="http://epr-ai-lno-p01.epri.com:8000/v1/chat/completions"


def generate_schema_from_llm(
        url: str,
        query: str = DEFAULT_QUERY,
) -> str:
    # 1) Download and prune HTML
    page = requests.get(url, timeout=10).text
    soup = BeautifulSoup(page, "lxml")
    snippet = soup.encode_contents().decode() if soup else page
    chunks = PruningContentFilter(threshold=0.5).filter_content(snippet)
    html_for_schema = "\n".join(chunks)

    # 2) Build messages

    system_message = {
            "role": "system", 
            "content": f"""You specialize in generating special JSON schemas for web scraping. This schema uses CSS or XPATH selectors to present a repetitive pattern in crawled HTML, such as a product in a product list or a search result item in a list of search results. We use this JSON schema to pass to a language model along with the HTML content to extract structured data from the HTML. The language model uses the JSON schema to extract data from the HTML and retrieve values for fields in the JSON schema, following the schema.

Generating this HTML manually is not feasible, so you need to generate the JSON schema using the HTML content. The HTML copied from the crawled website is provided below, which we believe contains the repetitive pattern.

# Schema main keys:
- name: This is the name of the schema.
- baseSelector: This is the CSS or XPATH selector that identifies the base element that contains all the repetitive patterns.
- baseFields: This is a list of fields that you extract from the base element itself.
- fields: This is a list of fields that you extract from the children of the base element. {{name, selector, type}} based on the type, you may have extra keys such as "attribute" when the type is "attribute".

# Extra Context:
In this context, the following items may or may not be present:
- Example of target JSON object: This is a sample of the final JSON object that we hope to extract from the HTML using the schema you are generating.
- Extra Instructions: This is optional instructions to consider when generating the schema provided by the user.
- Query or explanation of target/goal data item: This is a description of what data we are trying to extract from the HTML. This explanation means we're not sure about the rigid schema of the structures we want, so we leave it to you to use your expertise to create the best and most comprehensive structures aimed at maximizing data extraction from this page. You must ensure that you do not pick up nuances that may exist on a particular page. The focus should be on the data we are extracting, and it must be valid, safe, and robust based on the given HTML.

# What if there is no example of target JSON object and also no extra instructions or even no explanation of target/goal data item?
In this scenario, use your best judgment to generate the schema. You need to examine the content of the page and understand the data it provides. If the page contains repetitive data, such as lists of items, products, jobs, places, books, or movies, focus on one single item that repeats. If the page is a detailed page about one product or item, create a schema to extract the entire structured data. At this stage, you must think and decide for yourself. Try to maximize the number of fields that you can extract from the HTML.

# What are the instructions and details for this schema generation?
{prompt_template}"""
    }
    user_message = {
        "role": "user",
        "content": f"""
            HTML to analyze:
            ```html
            {html_for_schema}
            ```
            """
    }

        if query:
            user_message["content"] += f"\n\n## Query or explanation of target/goal data item:\n{query}"
        if target_json_example:
            user_message["content"] += f"\n\n## Example of target JSON object:\n```json\n{target_json_example}\n```"

        if query and not target_json_example:
            user_message["content"] += """IMPORTANT: To remind you, in this process, we are not providing a rigid example of the adjacent objects we seek. We rely on your understanding of the explanation provided in the above section. Make sure to grasp what we are looking for and, based on that, create the best schema.."""
        elif not query and target_json_example:
            user_message["content"] += """IMPORTANT: Please remember that in this process, we provided a proper example of a target JSON object. Make sure to adhere to the structure and create a schema that exactly fits this example. If you find that some elements on the page do not match completely, vote for the majority."""
        elif not query and not target_json_example:
            user_message["content"] += """IMPORTANT: Since we neither have a query nor an example, it is crucial to rely solely on the HTML content provided. Leverage your expertise to determine the schema based on the repetitive patterns observed in the content."""
        
        user_message["content"] += """IMPORTANT: Ensure your schema remains reliable by avoiding selectors that appear to generate dynamically and are not dependable. You want a reliable schema, as it consistently returns the same data even after many page reloads.

        Analyze the HTML and generate a JSON schema that follows the specified format. Only output valid JSON schema, nothing else.
        """

    # 3) Call internal LLM
    combined_prompt = system_msg + "\n\n" + user_content
    try:
        resp = perform_completion_with_backoff(
            provider=LLM_PROVIDER,
            prompt_with_variables=combined_prompt,
            json_response=True,
            api_token=LLM_API_TOKEN,
            base_url=LLM_BASE_URL,
        )
    except Exception as e:
        log.error(f"Internal LLM call failed: {e}")
        raise

    # 4) Parse output
    content = resp.choices[0].message.content
    try:
        schema = json.loads(content)
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse schema JSON: {e}\nOutput:\n{content}")
        raise

    log.info(f"Generated schema for {url}: {schema}")
    return schema




# def generate_schema_from_llm(
#     url: str,
#     query=DEFAULT_QUERY
# ) -> str:
#     page = requests.get(url).text
#     soup = BeautifulSoup(page, "lxml")
#     html_snippet = soup.encode_contents().decode() if soup else page
#     pruner = PruningContentFilter(threshold=0.5)
#     filtered_chunks = pruner.filter_content(html_snippet)
#     html_for_schema = "\n".join(filtered_chunks)

#     # llm_cfg = LLMConfig(
#     #     provider="openai/gpt-4o-mini",
#     #     api_token=os.getenv("OPENAI_API_KEY"),
#     #     temprature=0.0
#     # )
#     llm_cfg = LLMConfig(
#         # provider="meta/llama-3.2-90b-vision-instruct",
#         provider="google/gemma-3-27b-it",
#         base_url="http://epr-ai-lno-p01.epri.com:8000/v1/chat/completions",
#         api_token="null"
#     )
    
#     schema = JsonCssExtractionStrategy.generate_schema(
#         html=html_for_schema,
#         schema_type="CSS",
#         query=query,
#         target_json_example=json.dumps([{
#             "course_code": "BIOL 0280",
#             "course_title": "Biochemistry",
#             "course_description": "Lectures and recitation sections explore…"
#         }], indent=2),
#         llm_config=llm_cfg
#     )
    
#     return schema

async def generate_schema(
    source: SourceConfig,
) -> dict:
    log = logging.getLogger(__name__)
    raw = generate_schema_from_llm(source.schema_url)
    if isinstance(raw, str):
        schema = json.loads(raw)
    elif isinstance(raw, dict):
        schema = raw
    else:
        raise TypeError(f"Unexpected schema type: {type(raw)}")
    log.info(f"Generated schema for {source.name!r}:\n{schema}")
    return schema