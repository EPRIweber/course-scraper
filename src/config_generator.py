# src/config_generator.py
import asyncio
from collections import OrderedDict
import json
import os
import logging
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler, BM25ContentFilter
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.utils import get_content_of_website_optimized
from bs4 import BeautifulSoup
from httpx import HTTPStatusError

from crawl4ai import AsyncWebCrawler
import httpx

from src.crawler import crawl_and_collect_urls
from src.render_utils import fetch_page

from .llm_client import GemmaModel
from .prompts.catalog_urls import CatalogRootPrompt, CatalogSchemaPrompt

from .config import SourceConfig

logger = logging.getLogger(__name__)

_GOOGLE_SEARCH_SEM = asyncio.BoundedSemaphore(1)
# _FETCH_PAGE_SEM = asyncio.BoundedSemaphore(1)


GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX = os.getenv("GOOGLE_CX")

KEYWORDS = ["catalog", "bulletin", "course", "curriculum", "description", "current"]

async def discover_source_config(name: str, host: str = None) -> tuple[list[SourceConfig], int, int, list[str], list[str]]:
    # """Discover a ``SourceConfig`` for ``name``."""
    total_root  = total_schema = 0
    candidate_count = 0
    final_candidates = []

    candidates, root_errors, schema_errors = await discover_catalog_urls(name, host)
    for candidate in candidates:
        root, schema, root_usage, schema_usage = candidate

        total_root += root_usage
        total_schema += schema_usage

        pr = urlparse(root)
        ps = urlparse(schema)

        shared_domain = f"{pr.scheme}://{pr.netloc}"
        url_base_exclude = ""

        # if they're on the same host but schema isn't a sub‑path of root
        if pr.netloc == ps.netloc:
            root_path = pr.path.rstrip("/") + "/"
            if not ps.path.startswith(root_path):
                url_base_exclude = shared_domain
        
        candidate_count += 1
        final_candidates.append(SourceConfig(
            source_id=f"LOCAL_{name}",
            name=name + " src_" + str(candidate_count),
            root_url=root,
            schema_url=schema,
            url_base_exclude=url_base_exclude
        ))
    
    return final_candidates, total_root, total_schema, root_errors, schema_errors

async def discover_catalog_urls(school: str, host = None) -> Tuple[list[Tuple[str, str, int, int]], list[str], list[str]]:
    """Return root and schema URLs discovered for ``school``."""
    undergrad_query = f"{school} undergraduate course description catalog bulletin"
    grad_query = f"{school} graduate course description catalog bulletin"
    try:
        undergrad_results = await google_search(undergrad_query)
        grad_results = await google_search(grad_query)
    except Exception as e:
        logger.warning("Search failed for %s", school)
        raise e
    
    undergrad_filtered = filter_catalog_urls(undergrad_results)
    grad_filtered = filter_catalog_urls(grad_results)
    combined = undergrad_filtered + grad_filtered + undergrad_results + grad_results
    deduped = list(OrderedDict.fromkeys(combined))

    # pages = fetch_snippets(deduped)

    # 1) build a flat list of {url,snippet} dicts
    # pages: List[dict] = []
    root_url_errors = []
    root_urls = []
    seen = set()

    for hit in deduped:
        try:
            hit_domain = urlparse(hit).netloc
            # print(f"Searching {hit} for root url...")
            html = await fetch_page(hit, default_playwright=False)
            # print(f"HTML Recieved")
            soup = BeautifulSoup(html, 'html.parser')
            # find links to course description pages
            course_descr_links: List[str] = []
            for a in soup.find_all('a', href=True):
                # print(f"Examining link: {a['href']}")
                candidate_domain = urlparse(a['href']).netloc
                if candidate_domain and candidate_domain != hit_domain:
                    continue
                text = a.get_text(strip=True).lower()
                href = a['href']
                link = href.lower()
                if ('course' in text or 'course' in link) and 'archive' not in link\
                    and (host is None or host in link):
                    full_url = urljoin(hit, href)
                    course_descr_links.append(full_url)

            to_fetch = [hit] + course_descr_links

            fetch_deduped = list(OrderedDict.fromkeys(to_fetch))
            
            # print("Sending to fetch_snippets...")
            # fetch and append
            pages = await fetch_snippets(fetch_deduped, return_html=True)
            # print("fetch_snippets returned")

            # print("Sending to LLM...")
            root_url, root_usage = await llm_select_root(school, pages) or (None, 0)
            print(f'LLM returned root URL: {root_url}')
            # verify valid root_url
            pr = urlparse(root_url)
            if not pr.scheme or not pr.netloc:
                raise ValueError(f"Invalid root URL: {root_url}")
            
            if root_url not in seen:
                root_urls.append((root_url, root_usage))
            else:
                print("duplicate seen")
        except Exception as e:
            root_url_errors.append(f"Root Select Failed for Hit {hit}\n\nError: {e}")
    
    if not root_urls:
        raise Exception(f'No root URLs found for {school}. Found the following errors while processing hits: \n\n{"\n\n".join(root_url_errors)}')
    
    candidate_configs = []
    schema_gen_errors = []

    for root_url_tuple in root_urls:
        try:
            root_url, root_usage = root_url_tuple
            root_domain = urlparse(root_url).netloc
            html = await fetch_page(root_url, default_playwright=True)
            soup = BeautifulSoup(html, 'html.parser')

            # find links to course description pages
            course_descr_links: List[str] = []
            all_tags = soup.find_all('a', href=True)
            all_urls = []
            for a in all_tags:
                candidate_domain = urlparse(a['href']).netloc
                if candidate_domain and candidate_domain != root_domain:
                    continue
                text = a.get_text(strip=True).lower()
                href = a['href']
                full_path = urljoin(root_url, href)
                all_urls.append(full_path)
                if ('course' in text or 'course' in href.lower()) \
                    and 'archive' not in href.lower()\
                    and 'about' not in href.lower():
                    course_descr_links.append(full_path)

            combined = course_descr_links + all_urls
            deduped = list(OrderedDict.fromkeys(combined))
            schema_pages = await fetch_snippets(deduped)
            schema_url, schema_usage = await llm_select_schema(school, root_url, schema_pages) or (None, 0)
            print(f'LLM returned Schema URL: {schema_url}')
            if not schema_url:
                raise f"No schema URL returned for {school}"
            if root_url and schema_url:
                candidate_configs.append((root_url, schema_url, root_usage, schema_usage))
        except Exception as e:
            schema_gen_errors.append(f"Schema URL Finding Fail for {root_url}\n\nError: {e}")
            
    
    return candidate_configs, root_url_errors, schema_gen_errors

def make_markdown_run_cfg(timeout_s: int) -> CrawlerRunConfig:
    """Return a crawler run configuration for Markdown extraction."""
    return CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        markdown_generator=DefaultMarkdownGenerator(
            content_filter=PruningContentFilter(threshold=0.3),
            options={"ignore_links": True},
        ),
        page_timeout=timeout_s * 1000,
    )

async def fetch_snippets(
        urls: List[str],
        return_html: Optional[bool] = False
    ) -> List[dict]:
    """Fetch each URL via Playwright+HTTPX fallback and hand back raw HTML."""
    pages = []
    for url in urls:
        try:
            # print(f"Trying: {url}")
            html = await fetch_page(url, default_playwright=True)
            if return_html:
                pruner = PruningContentFilter(threshold=0.2)
                chunks = pruner.filter_content(html)
                chunks = filter(lambda chunk: chunk if chunk.strip() else None, chunks)
                snippet = "\n".join(chunks)
            else:
                snippet = get_content_of_website_optimized(url, html)
            pages.append({"url": url, "snippet": snippet})
            await asyncio.sleep(0.2)
        except Exception as e:
            logger.debug("Failed to fetch %s for snippet: %s", url, e)
    return pages

async def google_search(query: str, *, count: int = 3) -> List[str]:
    """Return a list of result URLs from Google Programmable Search."""
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        raise RuntimeError(
            "GOOGLE_API_KEY and GOOGLE_CX environment variables are required"
        )
    
    # query = query.replace("TESTING", "")

    async with _GOOGLE_SEARCH_SEM:
        params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": query, "num": count}
        async with httpx.AsyncClient(timeout=60000 * 10, verify=False) as client:
            try:
                resp = await client.get(GOOGLE_CSE_ENDPOINT, params=params)
                resp.raise_for_status()
            except HTTPStatusError as e:
                logger.warning("Google Search HTTP %s for query %r: %s", 
                               e.response.status_code, query, e)
                return []
            data = resp.json()
        return [item["link"] for item in data.get("items", []) if item.get("link")]

        #     resp = await client.get(GOOGLE_CSE_ENDPOINT, params=params)
        #     resp.raise_for_status()
        #     data = resp.json()
        # return [item["link"] for item in data.get("items", [])]

def filter_catalog_urls(urls: List[str]) -> List[str]:
    filtered = []
    for url in urls:
        lower = url.lower()
        if ('pdf' not in lower): # any(k in lower for k in KEYWORDS) and ".edu" in lower and 
            filtered.append(url)
    return filtered

async def llm_select_root(school: str, pages: List[dict]) -> tuple[str, int]:
    """Use the LLM to choose the best root URL from pre-fetched ``pages``."""
    # print("⟳ pages passed into llm_select_root:", pages)
    logger.debug("REACHED llm_select_root")
    if not pages:
        logger.warning("No pages provided to llm_select_root")
        raise Exception(f"No pages provided to llm_select_root for {school}")
    prompt = CatalogRootPrompt(school, pages)
    llm = GemmaModel()
    llm.set_response_format({
        "type": "json_object",
        "json_schema": {
            "name": "catalog_selection",
            "description": "course_catalog_url_selecting",
            "root_url": {
                "type": "string"
            },
            "strict": True
        }
    })
    try:
        sys_p = prompt.system()
        user_p = prompt.user()
        resp = llm.chat(
            [
                {"role": "system", "content": sys_p},
                {"role": "user", "content": user_p[:min(250_000, len(user_p))]},
            ]
        )
        # print(f"SYSTEM PROMPT:\n{sys_p}\n\n\u25B6 USER PROMPT:\n{user_p}\n")
        data = json.loads(resp["choices"][0]["message"]["content"])
        if isinstance(data, dict):
            try:
                # print(data)
                url = data.get("root_url")
            except Exception as ex:
                logger.warning(f"Failed to load URL from LLM response, instead received: {data}")
                raise ex
        else:
            logger.warning("Root LLM returned non-list data: %s", data)
            raise Exception("Root LLM returned non-list data: %s", data)
        # print(resp)
        prompt_t = resp.get("usage", {}).get("prompt_tokens")
        completion_t = resp.get("usage", {}).get("completion_tokens")


        return url, prompt_t + completion_t
    except Exception as e:
        logger.warning("LLM root selection failed for %s", school)
        raise e

async def llm_select_schema(
    school: str,
    root_url: str,
    pages: List[str]
) -> tuple[str, int]:
    prompt = CatalogSchemaPrompt(school, root_url, pages)
    sys_p = prompt.system()
    user_p = prompt.user()
    # print(f"SYSTEM PROMPT:\n{sys_p}\n\n\u25B6 USER PROMPT:\n{user_p}\n")
    llm = GemmaModel()
    llm.set_response_format({
        "type": "json_object",
        "json_schema": {
            "name": "CourseExtractionSchema",
            "description": "CourseExtractionSchema",
            "schema_url": {
                "type": "string"
            },
            "strict": True
        }
    })
    try:
        resp = llm.chat(
            [
                {"role": "system", "content": sys_p},
                {"role": "user", "content": user_p[:min(250_000, len(user_p))]},
            ]
        )
        data = json.loads(resp["choices"][0]["message"]["content"])
        if isinstance(data, dict):
            try:
                # print(data)
                url = data.get("schema_url")
            except Exception as ex:
                logger.warning(f"Failed to load URL from LLM response, instead received: {data}")
                raise ex
        else:
            logger.warning("Schema LLM returned non-list data: %s", data)
            raise Exception("Schema LLM returned non-list data: %s", data)
        # print(resp)
        prompt_t = resp.get("usage", {}).get("prompt_tokens")
        completion_t = resp.get("usage", {}).get("completion_tokens")

        return url, prompt_t + completion_t
    except Exception as e:
        logger.warning("LLM schema selection failed for %s", school)
        raise e