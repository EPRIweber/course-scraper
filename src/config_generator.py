# src/config_generator.py

from __future__ import annotations

import asyncio
from collections import OrderedDict
import json
import logging
import os
from pprint import pprint
import random
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from httpx import HTTPStatusError
from lxml import etree
from playwright.async_api import async_playwright

from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.utils import get_content_of_website_optimized

from .config import SourceConfig
from .llm_client import GPTModel
from .prompts.catalog_urls import CatalogRootPrompt, CatalogSchemaPrompt
from .render_utils import fetch_page

logger = logging.getLogger(__name__)

# --- Constants / Globals ------------------------------------------------------
GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX = os.getenv("GOOGLE_CX")

_GOOGLE_SEARCH_SEM = asyncio.BoundedSemaphore(1)

# NOTE: Keep KEYWORDS as-is even if not used in filter predicate
KEYWORDS = ["catalog", "bulletin", "course", "curriculum", "description", "current"]

DEBUG = True


# --- Public API ---------------------------------------------------------------
async def discover_source_config(name: str, host: str | None = None) -> tuple[list[SourceConfig], int, int, list[str], list[str]]:
  """Discover one or more SourceConfig objects for the given school name.

  Returns (candidates, total_root_tokens, total_schema_tokens, root_errors, schema_errors).
  Observable behavior preserved; includes url_base_exclude calculation.
  """
  candidate_count = 0
  final_candidates: list[SourceConfig] = []

  # NOTE: discover_catalog_urls now returns totals instead of per-config usage
  candidates, root_errors, schema_errors, pdf_configs, total_root, total_schema, max_depth = await discover_catalog_urls(name, host)

  for root, schema in candidates:
    pr = urlparse(root)
    ps = urlparse(schema)
    url_base_exclude = ""

    # If same host but schema path is not under root path -> exclude base host
    if pr.netloc == ps.netloc:
      root_path = (pr.path.rstrip("/") + "/") if pr.path else "/"
      if not (ps.path or "/").startswith(root_path):
        url_base_exclude = f"{pr.scheme}://{pr.netloc}"
    
    new_src = SourceConfig(
      source_id=f"LOCAL_{name}",
      name=f"{name} src_{candidate_count}",
      type="html",
      root_url=root,
      schema_url=schema,
      url_base_exclude=url_base_exclude
    )

    if max_depth:
      new_src.crawl_depth = max_depth

    final_candidates.append(new_src)
    candidate_count += 1

  for root, schema in pdf_configs:
    final_candidates.append(
      SourceConfig(
        source_id=f"LOCAL_{name}",
        name=f"{name} src_{candidate_count}",
        type="pdf",
        root_url=root,
        schema_url=schema,
      )
    )
    candidate_count += 1

  return final_candidates, int(total_root or 0), int(total_schema or 0), root_errors, schema_errors

# --- Core discovery -----------------------------------------------------------
async def discover_catalog_urls(
  school: str,
  host: str | None = None,
) -> Tuple[list[Tuple[str, str]], list[str], list[str], list[Tuple[str, str]], int, int, Optional[int]]:
  """Return discovered candidate configs and error collections for `school`.

  Return shape:
    (candidate_configs, root_url_errors, schema_gen_errors, pdf_configs, total_root_usage, total_schema_usage, max_depth)
  where each candidate_config is (root_url, schema_url). Token usage is returned as totals.
  """

  queries = [
    f"{school} undergraduate course description current catalog",
    f"{school} graduate course description current catalog",
  ]

  root_url_errors: list[str] = []
  schema_gen_errors: list[str] = []

  query_results: list[list[str]] = []
  for q in queries:
    try:
      hits = await google_search(q)
      # hits = []
      pass
    except Exception as e:
      logger.warning("Search failed for %s with %r: %s", school, q, e)
      hits = []
    query_results.append(hits)
  
  if not any(query_results):
    raise Exception(f"No search results found for {school} with queries: {queries}")

  # We no longer store per-config usage; only totals per school
  total_root_usage = 0
  total_schema_usage = 0

  root_urls: list[str] = []  # discovered HTML root URLs (non-PDF)
  candidate_configs: list[Tuple[str, str]] = []
  pdf_configs: list[Tuple[str, str]] = []
  seen_roots: set[str] = set()
  mc_found = False
  max_depth: Optional[int] = None

  if DEBUG:
    print(f"Search Results:\n {query_results}")

  # Helper: robust Modern Campus detection
  def _looks_like_modern_campus(hit_url: str, html_text: str) -> bool:
    if "#select_catalog" in html_text or 'id="select_catalog"' in html_text:
      return True
    u = urlparse(hit_url)
    if u.netloc.startswith("catalog.") or "content.php" in u.path or "catoid=" in (u.query or ""):
      return True
    return False

  for results in query_results:
    if mc_found:
      break
    if not results:
      continue

    filtered = filter_catalog_urls(results, host)
    deduped_hits = list(OrderedDict.fromkeys(filtered + results))
    # seen = set()

    for hit in deduped_hits:
      if mc_found:
        break
      try:
        # seen.add(hit)
        html = await fetch_page(hit, default_playwright=True)
        soup = BeautifulSoup(html or "", "html.parser")
        page_text = soup.decode() if soup else ""
        txt_lower = soup.get_text(" ", strip=True).lower()
        # print(f"Raw HTML: {html}...")  # NEW DEBUG PRINT

        # --- Modern Campus branch ---
        if ("modern campus" in txt_lower):
          mc_candidates, mc_root_errs, mc_schema_errs, mc_pdfs = await process_modern_campus(hit, host)
          root_url_errors.extend(mc_root_errs)
          schema_gen_errors.extend(mc_schema_errs)

          if mc_candidates:
            for r, s in mc_candidates:
              if r not in seen_roots:
                seen_roots.add(r)
                candidate_configs.append((r, s))
          if mc_pdfs:
            for r, s in mc_pdfs:
              pdf_configs.append((r, s))

          if candidate_configs or pdf_configs:
            mc_found = True
            max_depth = 100  # keep your previous behavior for MC depth
            break  # short-circuit: MC gives everything we need
        else:
          continue  # skip non-MC for now
        # --- Non-MC path (unchanged behavior except usage now totals) ---
        course_descr_links: list[str] = []
        hit_domain = urlparse(hit).netloc
        for a in soup.find_all("a", href=True):
          href = a["href"]
          # print(f"Found href: {href}")  # NEW DEBUG PRINT
          text = a.get_text(strip=True).lower()
          candidate_domain = urlparse(href).netloc
          link = href if candidate_domain else urljoin(hit, href)
          # NEW DEBUG PRINT
          # print(f"Joined Link: {link}")
          if host and (host not in link):
            continue
          if candidate_domain and candidate_domain != hit_domain:
            continue
          # if link in seen:
          #   continue
          if (
            ("course" in text or "graduate" in text or "course" in href.lower())
            and ("archive" not in link)
            and ("about" not in link)
          ):
            # seen.add(link)
            course_descr_links.append(link)
          # if (
          #    "catalog" in text or "bulletin" in text 
          # ):
          #   if link not in deduped_hits:
          #     seen.add(link)
          #     deduped_hits.append(link)

        to_fetch = list(OrderedDict.fromkeys(course_descr_links + [hit]))
        pages = await fetch_snippets(to_fetch, return_html=True)

        root_info, root_usage = await llm_select_root(school, pages)
        total_root_usage += int(root_usage or 0)

        pdf_flag = False
        links: list[str] = []
        if isinstance(root_info, dict):
          links = list(root_info.get("links", []) or [])
          pdf_flag = bool(root_info.get("pdf_flag", False))
        else:
          raise ValueError(f"Unexpected root_url format: {root_info!r}")

        if not links:
          root_url_errors.append(f"No Root Selected for {hit}\nLLM Returned: {root_info}")
          continue

        if pdf_flag:
          print(f"PDF link selected for {hit}")
          for l in links:
            pdf_configs.append((l, l))
          continue

        for link in links:
          pr = urlparse(link)
          if not pr.scheme or not pr.netloc:
            root_url_errors.append(f"Invalid root URL: {link}")
            continue
          if "coursedog" in link:
            root_url_errors.append(f"Course Dog Unscrapable Site: {link}")
            continue
          if host and (host not in link.lower()):
            continue
          if link not in seen_roots:
            seen_roots.add(link)
            root_urls.append(link)

      except Exception as e:
        root_url_errors.append(f"Root Select Failed for Hit {hit}\n\nError: {e}")
        logger.debug("Root selection failure for %s: %s", hit, e)

  if mc_found:
    print("Returning Early with Modern Campus Results")
    return candidate_configs, root_url_errors, schema_gen_errors, pdf_configs, total_root_usage, total_schema_usage, max_depth

  # Legacy final error behavior for non-MC path
  if not root_urls or not pdf_configs:
    raise Exception(
      f"No root URLs for PDFs found for {school}. Found the following errors while processing hits: \n\n{'\n\n'.join(root_url_errors)}"
    )

  print(f"Attempting to find schma URLs for {len(root_urls)} root urls...")
  # Schema selection for non-MC roots
  for root_url in root_urls:
    try:
      root_domain = urlparse(root_url).netloc
      html = await fetch_page(root_url, default_playwright=True)
      soup = BeautifulSoup(html or "", "html.parser")

      all_urls: list[str] = []
      for a in soup.find_all("a", href=True):
        href = a["href"]
        full = str(urljoin(root_url, href)).lower()
        candidate_domain = urlparse(href).netloc
        if (
          (candidate_domain and candidate_domain != root_domain)
          or 'archive' in full
        ):
          continue
        if host and (host not in full):
          continue
        all_urls.append(full)

      random.shuffle(all_urls)
      schema_candidates = list(OrderedDict.fromkeys(all_urls))
      pages = await fetch_snippets(schema_candidates[:35], return_html=False)
      schema_url, schema_usage = await llm_select_schema(school, root_url, pages) or (None, 0)
      total_schema_usage += int(schema_usage or 0)
      if not schema_url:
        raise RuntimeError(f"No schema URL returned for {school} using root {root_url}")

      candidate_configs.append((root_url, schema_url))
    except Exception as e:
      schema_gen_errors.append(f"Schema URL Finding Fail for {root_url}\n\nError: {e}")
      logger.debug("Schema selection failure for %s: %s", root_url, e)

  return candidate_configs, root_url_errors, schema_gen_errors, pdf_configs, total_root_usage, total_schema_usage, max_depth

# --- Modern Campus flow -------------------------------------------------------
async def process_modern_campus(
  hit: str,
  host: str | None = None,
) -> Tuple[list[Tuple[str, str]], list[str], list[str], list[Tuple[str, str]]]:
  """Modern Campus discovery without LLMs.

  Returns (candidate_configs, root_errors, schema_errors, pdf_configs),
  with each candidate as (root_url, schema_url). No per-config usage values.
  """
  root_errors: list[str] = []
  schema_errors: list[str] = []
  pdf_configs: list[Tuple[str, str]] = []
  candidate_configs: list[Tuple[str, str]] = []

  async def _anchors_eval(page) -> list[dict]:
    try:
      return await page.evaluate(
        """
        () => Array.from(document.querySelectorAll('a[href]')).map(a => ({
          href: a.getAttribute('href') || '',
          abs: a.href,
          text: (a.textContent||'').trim().toLowerCase(),
          onclick: a.getAttribute('onclick') || ''
        }))
        """
      )
    except Exception:
      return []

  async def _extract_roots_from_dom(page, current_url: str, host_filter: Optional[str]) -> list[str]:
    anchors = await _anchors_eval(page)
    roots: list[str] = []
    for a in anchors:
      href = a.get('href') or ''
      absu = a.get('abs') or ''
      text = a.get('text') or ''
      if not href and not absu:
        continue
      link = absu or (href if urlparse(href).netloc else urljoin(current_url, href))
      if host_filter and (host_filter not in (link or '').lower()):
        continue
      if ('course description' in text) or ('courses description' in text) or ('course' in text):
        roots.append(link)
    return list(OrderedDict.fromkeys(roots))

  async def _safe_select(page, value: str):
    try:
      async with page.expect_navigation(wait_until='networkidle', timeout=5000):
        await page.select_option('#select_catalog', value)
    except Exception:
      await page.select_option('#select_catalog', value)
      await page.wait_for_load_state('networkidle')
      await asyncio.sleep(0.5)

  # NEW: robust year extraction and fallback selection helpers
  import re
  def _extract_years(label: str) -> list[int]:
    """
    Extract probable years from option text.
    Handles '2024–2025', '2024-25', '2024/25', '2023-2024', etc.
    Returns a list of 4-digit years; if only a two-digit second year, expand relative to the first.
    """
    t = (label or "").lower().replace('–', '-').replace('—', '-').replace('—', '-').replace(' to ', '-').replace('/', '-')
    # Find all 4-digit years first
    years = [int(y) for y in re.findall(r'\b(19|20)\d{2}\b', t)]
    # Handle compact forms like '2024-25'
    m = re.search(r'\b((19|20)\d{2})\s*-\s*(\d{2})\b', t)
    if m:
      y1 = int(m.group(1))
      y2_two = int(m.group(3))
      # Expand '25' to 2025 by decade alignment
      y2 = (y1 // 100) * 100 + y2_two
      if y2 < y1:
        y2 += 100  # rare wrap
      if y1 not in years:
        years.append(y1)
      if y2 not in years:
        years.append(y2)
    # Deduplicate & sort
    years = sorted(set(years))
    return years

  def _is_undergrad(label: str) -> bool:
    return 'undergraduate' in (label or '').lower()

  def _is_grad(label: str) -> bool:
    return 'graduate' in (label or '').lower()

  try:
    async with async_playwright() as pw:
      browser = await pw.chromium.launch(headless=True)
      page = await browser.new_page()
      await page.goto(hit, wait_until='networkidle')

      try:
        options = await page.eval_on_selector_all(
          '#select_catalog option',
          "els => els.map(o => ({value:o.value, text:(o.textContent||'').trim()}))",
        )
      except Exception as e:
        options = []
        root_errors.append(f"Failed to read #select_catalog options at {hit}: {e}")

      # NEW: choose year group with fallback
      # Build a structure: [(years_list, text, value)]
      parsed = [(_extract_years(o.get('text','')), o.get('text',''), o.get('value','')) for o in options]
      # Prefer any option where years include 2024 and 2025
      group_2425 = [p for p in parsed if (2024 in p[0] and 2025 in p[0])]
      # Else any option that includes 2024
      group_24 = [p for p in parsed if 2024 in p[0]]

      chosen_group = None
      if group_2425:
        chosen_group = group_2425
      elif group_24:
        chosen_group = group_24
      else:
        # Fallback: pick the most recent year found across all options
        # Determine each option's "max year", pick the max over all, then keep that group.
        with_max = [(max(yrs) if yrs else -1, txt, val, yrs) for (yrs, txt, val) in parsed]
        most_recent = max((m for (m, _, _, _) in with_max), default=-1)
        # If still nothing matched (no year at all), just keep all options (last resort)
        if most_recent == -1:
          chosen_group = parsed
        else:
          chosen_group = [(yrs, txt, val) for (yrs, txt, val) in parsed if (yrs and max(yrs) == most_recent)]

      # Within chosen year group, if both UG and GR are present, select both types; else select all in the group.
      lower = [dict(value=val, text=txt) for (yrs, txt, val) in chosen_group if _is_undergrad(txt)]
      upper = [dict(value=val, text=txt) for (yrs, txt, val) in chosen_group if _is_grad(txt)]
      if lower and upper:
        chosen = lower + upper
      else:
        chosen = [dict(value=val, text=txt) for (yrs, txt, val) in chosen_group]

      # Collect candidate root links
      root_links: list[str] = []
      if chosen:
        for opt in chosen:
          try:
            await _safe_select(page, opt['value'])
            root_links.extend(await _extract_roots_from_dom(page, page.url, host))
          except Exception as e:
            root_errors.append(f"Failed selecting catalog '{opt}': {e}")
      else:
        root_links.extend(await _extract_roots_from_dom(page, page.url, host))

      root_links = list(OrderedDict.fromkeys(root_links))

      # For each root, find schema_url via showCourse/preview_course_nopop
      for root_url in root_links:
        try:
          await page.goto(root_url, wait_until='networkidle')
          anchors = await _anchors_eval(page)

          schema_url: Optional[str] = None

          for a in anchors:
            if 'showcourse' in (a.get('onclick','').lower()):
              href = a.get('href') or ''
              absu = a.get('abs') or ''
              schema_url = absu or (href if urlparse(href).netloc else urljoin(root_url, href))
              break

          if not schema_url:
            for a in anchors:
              href = a.get('href') or ''
              if 'preview_course_nopop.php' in href.lower():
                absu = a.get('abs') or ''
                schema_url = absu or (href if urlparse(href).netloc else urljoin(root_url, href))
                break

          if schema_url:
            candidate_configs.append((root_url, schema_url))

        except Exception as e:
          schema_errors.append(f"Failed schema discovery for MC root {root_url}: {e}")

      await browser.close()

  except Exception as e:
    root_errors.append(f"Modern Campus processing failed for {hit}: {e}")

  return candidate_configs, root_errors, schema_errors, pdf_configs

# --- Helpers -----------------------------------------------------------------
async def fetch_snippets(urls: List[str], return_html: Optional[bool] = False) -> List[dict]:
  """Fetch each URL using Playwright by default; return pruned snippets or optimized text.

  - Always uses fetch_page(url, default_playwright=True)
  - If return_html=True: prune with PruningContentFilter(threshold=0.2)
  - Else: get_content_of_website_optimized(url, html)
  - Sleep 0.2s between items
  """
  pages: list[dict] = []
  for url in urls:
    try:
      html = await fetch_page(url, default_playwright=True)
      if return_html:
        pruner = PruningContentFilter(threshold=0.2)
        chunks = pruner.filter_content(html)
        chunks = [c for c in chunks if c and c.strip()]
        snippet = "\n".join(chunks)
      else:
        snippet = get_content_of_website_optimized(url, html)
      pages.append({"url": url, "snippet": snippet})
      await asyncio.sleep(0.2)
    except Exception as e:
      logger.debug("Failed to fetch %s for snippet: %s", url, e)
  return pages


async def google_search(query: str, *, count: int = 3) -> List[str]:
  """Return a list of result URLs from Google Programmable Search.

  Preserves: count=3, semaphore=1, AsyncClient(timeout=60000*10, verify=False),
  HTTPStatusError -> return []
  """
  if not GOOGLE_API_KEY or not GOOGLE_CX:
    raise RuntimeError("GOOGLE_API_KEY and GOOGLE_CX environment variables are required")

  async with _GOOGLE_SEARCH_SEM:
    params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": query, "num": count}
    async with httpx.AsyncClient(timeout=60000 * 10, verify=False) as client:
      try:
        resp = await client.get(GOOGLE_CSE_ENDPOINT, params=params)
        resp.raise_for_status()
      except HTTPStatusError as e:
        logger.warning("Google Search HTTP %s for query %r: %s", e.response.status_code, query, e)
        return []
      data = resp.json()
    return [item["link"] for item in data.get("items", []) if item.get("link")]


def filter_catalog_urls(urls: List[str], host: str | None = None) -> List[str]:
  """Filtering semantics preserved exactly:
  - If host provided, only keep URLs where host in url.lower()
  - Otherwise keep all
  - Keep KEYWORDS list intact (even if unused here)
  """
  out: list[str] = []
  for u in urls:
    low = (u or "").lower()
    if (host in low if host else True):
      out.append(u)
  return out


async def llm_select_root(school: str, pages: List[dict]) -> tuple[dict, int]:
  """Use OSS+CatalogRootPrompt to select root links.

  Returns (root_obj, usage) where root_obj = {"links": [...], "pdf_flag": bool}.
  Token accounting preserved: sum(prompt_tokens + completion_tokens).
  """
  if not pages:
    raise Exception(f"No pages provided to llm_select_root for {school}")

  prompt = CatalogRootPrompt(school, pages)
  llm = GPTModel()
  llm.set_response_format(
    {
      "type": "json_object",
      "json_schema": {
        "name": "catalog_selection",
        "description": "course_catalog_url_selecting",
        "root_url": {
          "type": "object",
          "properties": {
            "links": {"type": "array", "items": {"type": "string", "format": "uri"}},
            "pdf_flag": {"type": "boolean"},
          },
          "required": ["links", "pdf_flag"],
        },
        "strict": True,
      },
    }
  )

  sys_p = prompt.system()
  user_p = prompt.user()
  resp = llm.chat(
    [
      {"role": "system", "content": sys_p},
      {"role": "user", "content": user_p[: min(240_000, len(user_p))]},
    ]
  )

  data = json.loads(resp["choices"][0]["message"]["content"])  # expected dict
  if not isinstance(data, dict):
    raise Exception(f"Root LLM returned non-dict data: {data}")

  url_obj = data.get("root_url")
  prompt_t = (resp.get("usage", {}) or {}).get("prompt_tokens")
  completion_t = (resp.get("usage", {}) or {}).get("completion_tokens")
  usage = int((prompt_t or 0) + (completion_t or 0))
  return url_obj, usage


async def llm_select_schema(school: str, root_url: str, pages: List[dict]) -> tuple[str, int]:
  """Use OSS+CatalogSchemaPrompt to select a representative schema URL.

  Returns (schema_url, usage). Token accounting preserved.
  """
  prompt = CatalogSchemaPrompt(school, root_url, pages)
  llm = GPTModel()
  llm.set_response_format(
    {
      "type": "json_object",
      "json_schema": {
        "name": "CourseExtractionSchema",
        "description": "CourseExtractionSchema",
        "schema_url": {"type": "string"},
        "strict": True,
      },
    }
  )

  sys_p = prompt.system()
  user_p = prompt.user()
  resp = llm.chat(
    [
      {"role": "system", "content": sys_p},
      {"role": "user", "content": user_p[: min(240_000, len(user_p))]},
    ]
  )

  data = json.loads(resp["choices"][0]["message"]["content"])  # expected dict
  if not isinstance(data, dict):
    raise Exception(f"Schema LLM returned non-dict data: {data}")

  url = data.get("schema_url")
  prompt_t = (resp.get("usage", {}) or {}).get("prompt_tokens")
  completion_t = (resp.get("usage", {}) or {}).get("completion_tokens")
  usage = int((prompt_t or 0) + (completion_t or 0))
  return url, usage


# --- Optional: local smoke test (commented) ----------------------------------
async def _smoke_test():
  mc_url = "https://catalog.wmich.edu/"  # replace with known Modern Campus URL
  res = await process_modern_campus(mc_url)
  pprint(res)

if __name__ == "__main__":
  import asyncio
  asyncio.run(_smoke_test())
