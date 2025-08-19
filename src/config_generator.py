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
from .render_utils import close_playwright, fetch_page

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
async def discover_source_config(
    name: str,
    host: str | None = None,
    presearch_results: list[list[str]] | None = None
) -> tuple[list[SourceConfig], int, int, list[str], list[str]]:
  """Discover one or more SourceConfig objects for the given school name.

  Returns (candidates, total_root_tokens, total_schema_tokens, root_errors, schema_errors).
  Observable behavior preserved; includes url_base_exclude calculation.
  """
  candidate_count = 0
  final_candidates: list[SourceConfig] = []

  # NOTE: discover_catalog_urls now returns totals instead of per-config usage
  candidates, root_errors, schema_errors, pdf_configs, total_root, total_schema, max_depth = await discover_catalog_urls(
    name,
    host,
    presearch_results
  )

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
  presearch_results: list[list[str]] | None = None,
) -> Tuple[list[Tuple[str, str]], list[str], list[str], list[Tuple[str, str]], int, int, Optional[int]]:
  """Return discovered candidate configs and error collections for `school`.

  Return shape:
    (candidate_configs, root_url_errors, schema_gen_errors, pdf_configs, total_root_usage, total_schema_usage, max_depth)
  where each candidate_config is (root_url, schema_url). Token usage is returned as totals.
  """

  root_url_errors: list[str] = []
  schema_gen_errors: list[str] = []
  
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
    print(f"Search Results:\n {presearch_results}")

  # Helper: robust Modern Campus detection
  def _looks_like_modern_campus(hit_url: str, html_text: str) -> bool:
    if "#select_catalog" in html_text or 'id="select_catalog"' in html_text:
      return True
    u = urlparse(hit_url)
    if u.netloc.startswith("catalog.") or "content.php" in u.path or "catoid=" in (u.query or ""):
      return True
    return False

  for results in presearch_results:
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
        origin_links = ' '.join([a.get("href") for a in soup.find_all("a", href=True)])
        page_text = soup.decode() if soup else ""
        txt_lower = soup.get_text(" ", strip=True).lower()
        # print(f"Raw HTML: {html}...")  # NEW DEBUG PRINT

        # --- Modern Campus branch ---
        if ("modern campus" in txt_lower or 'content.php?catoid=' in origin_links):
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
            max_depth = 100
          else:
            root_url_errors.append(f"Modern Campus Site Fail at {hit}")
          # break  # short-circuit: MC gives everything we need
        else:
          root_url_errors.append(f"Modern Campus site not found at {hit}")
          
          # ---------------------------
          # PDF branch (added)
          # ---------------------------
          pdfs = process_pdf(html, base_url=hit, allowed_host=host)
          if not pdfs:
            root_url_errors.append(f"No PDF catalog links found at {hit}")
            continue

          # Add a few best candidates. Each PDF is both root and schema.
          # (Keeps your downstream expectations unchanged.)
          for pdf_link in pdfs[:5]:
            pdf_configs.append((pdf_link, pdf_link))
            logger.info(f"[{school}] PDF catalog candidate: {pdf_link}")
          continue
            
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

  if mc_found or pdf_configs:
    print("Returning Early with Modern Campus Results")
    return candidate_configs, root_url_errors, schema_gen_errors, pdf_configs, total_root_usage, total_schema_usage, max_depth

  # Legacy final error behavior for non-MC path
  if not root_urls or not pdf_configs:
    errors = '\n\n'.join(root_url_errors)
    raise Exception(
      f"No root URLs for PDFs found for {school}. Found the following errors while processing hits: \n\n{errors}"
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
        () => Array.from(document.querySelectorAll('a[href]')).map(a => {
          const text  = (a.textContent || '').trim().toLowerCase();
          const title = (a.getAttribute('title') || '').trim().toLowerCase();
          const aria  = (a.getAttribute('aria-label') || '').trim().toLowerCase();
          const imgAlt = Array.from(a.querySelectorAll('img[alt]'))
                              .map(i => (i.getAttribute('alt') || '').trim().toLowerCase())
                              .join(' ');
          return {
            href: a.getAttribute('href') || '',
            abs: a.href,
            text,
            title,
            aria,
            imgAlt,
            onclick: a.getAttribute('onclick') || ''
          };
        })
        """
      )
    except Exception:
      return []
  
  async def _extract_roots_from_dom(page, current_url: str, host_filter: Optional[str], fallback: Optional[bool] = False) -> list[str]:
    anchors = await _anchors_eval(page)

    def _looks_mc_link(u: str) -> bool:
      s = (u or "").lower()
      return (
        "content.php" in s or
        "catoid=" in s or
        "preview_course_nopop.php" in s or
        "acalog" in s
      )

    roots: list[str] = []
    for a in anchors:
      href   = a.get('href')   or ''
      absu   = a.get('abs')    or ''
      text   = a.get('text')   or ''
      title  = a.get('title')  or ''
      aria   = a.get('aria')   or ''
      imgAlt = a.get('imgAlt') or ''
      if not href and not absu:
        continue

      link = absu or (href if urlparse(href).netloc else urljoin(current_url, href))
      link_l = (link or '').lower()

      # hard fallback: accept any MC category/tile like ...content.php?...catoid=...
      if fallback and ('content.php' in link_l and 'catoid=' in link_l) and 'preview_course_nopop' not in link_l:
        roots.append(link)
        continue

      # RELAXED host filtering for Modern Campus targets
      if host_filter and (host_filter.lower() not in link_l):
        # allow MC-shaped links even if host doesn't match (catalog subdomains, alias domains)
        if not (_looks_mc_link(href) or _looks_mc_link(absu)):
          continue

      # combine visible labels (handles image-only anchors)
      label = f"{text} {title} {aria} {imgAlt}".strip().lower()

      # keep simple text heuristics
      if ('course description' in label) or ('courses description' in label) or ('course' in label):
        roots.append(link)

    return list(OrderedDict.fromkeys(roots))

  async def _safe_select(page, value: str):
    import re
    try:
        # Most MC pages navigate; 'load' is sufficient and doesn't hang like 'networkidle'
        async with page.expect_navigation(wait_until='load', timeout=10000):
            await page.select_option('#select_catalog', value)
    except Exception:
        # Fallback: select without expecting navigation, then look for either URL change or a catalog marker
        await page.select_option('#select_catalog', value)
        try:
            # Many MC URLs include ?catoid=<value>
            await page.wait_for_url(re.compile(rf'catoid={value}\b'), timeout=7000)
        except Exception:
            # Final fallback: just ensure the page finished loading
            await page.wait_for_load_state('load', timeout=7000)
    await asyncio.sleep(0.3)


  # --- helpers to normalize and pick exactly ONE year group ---
  import re
  def _year_key(label: str) -> Optional[Tuple[int, int]]:
    """
    Normalize a year label into (start, end). Handles:
      '2024–2025', '2024-25', '2024/25', '2023-2024', '2024', etc.
    Returns None if no year found.
    """
    t = (label or "").lower().replace('–','-').replace('—','-').replace('/', '-').replace(' to ', '-')
    # exact range YYYY-YYYY
    m = re.search(r'\b((19|20)\d{2})\s*-\s*((19|20)\d{2})\b', t)
    if m:
      y1, y2 = int(m.group(1)), int(m.group(3))
      return (y1, y2)
    # compact range YYYY-YY
    m = re.search(r'\b((19|20)\d{2})\s*-\s*(\d{2})\b', t)
    if m:
      y1 = int(m.group(1))
      y2_two = int(m.group(3))
      y2 = (y1 // 100) * 100 + y2_two
      if y2 < y1:
        y2 += 100
      return (y1, y2)
    # single year
    m = re.search(r'\b((19|20)\d{2})\b', t)
    if m:
      y = int(m.group(1))
      return (y, y)
    return None

  def _is_undergrad(label: str) -> bool:
    first = 'undergraduate' in (label or '').lower()
    return first or 'undergrad' in (label or '').lower()

  def _is_grad(label: str) -> bool:
    first =  'graduate' in (label or '').lower()
    return first or 'grad' in (label or '').lower()

  try:
    async with async_playwright() as pw:
      browser = await pw.chromium.launch(headless=True)
      page = await browser.new_page()
      await page.goto(hit, wait_until='load')

      try:
        # Preserve option order; we will use the first target match by this order
        options = await page.eval_on_selector_all(
          '#select_catalog option',
          "els => els.map(o => ({value:o.value, text:(o.textContent||'').trim()}))",
        )
        if len(options) <= 1:
          try:
              # open the Select2 so it populates the hidden <select>
              await page.click('span.select2-selection--single')
              await page.wait_for_selector('.select2-results__option', timeout=5000)
              await asyncio.sleep(0.2)  # give it a beat to sync
              options = await page.eval_on_selector_all(
                  '#select_catalog option',
                  "els => els.map(o => ({value:o.value, text:(o.textContent||'').trim()}))",
              )
          except Exception:
              pass

      except Exception as e:
        options = []
        root_errors.append(f"Failed to read #select_catalog options at {hit}: {e}")

      # Build [(idx, key, text, value)]
      parsed: list[Tuple[int, Optional[Tuple[int,int]], str, str]] = [
        (i, _year_key(o.get('text','')), o.get('text',''), o.get('value','')) for i, o in enumerate(options)
      ]

      # --- choose exactly ONE year key ---
      # 1) first option in DOM order whose key == (2024,2025)
      target_key: Optional[Tuple[int,int]] = None
      for _, key, _, _ in parsed:
        if key == (2024, 2025):
          target_key = key
          break
      # 2) else first option in DOM order whose key includes 2024 (single or range)
      if target_key is None:
        for _, key, _, _ in parsed:
          if key and (key[0] == 2024 or key[1] == 2024):
            target_key = key
            break
      # 3) else fallback to the most recent year key present (by max(end_year)); pick the first such key in DOM order
      if target_key is None:
        # find the maximum end year across all keys
        keys = [k for _, k, _, _ in parsed if k is not None]
        if keys:
          max_end = max(k[1] for k in keys)
          # pick the first option in DOM order that has end == max_end
          for _, key, _, _ in parsed:
            if key and key[1] == max_end:
              target_key = key
              break

      # If we still have no key, just don't select anything; fall back to scanning current page
      chosen_opts: list[dict] = []
      if target_key is not None:
        # collect all options that share this exact key (i.e., same year group)
        group = [dict(value=val, text=txt) for _, key, txt, val in parsed if key == target_key]
        # within that group: if both UG & GR exist, keep both; else keep the first by DOM order
        lower = [o for o in group if _is_undergrad(o['text'])]
        upper = [o for o in group if _is_grad(o['text'])]
        if lower and upper:
          chosen_opts = lower + upper
        else:
          chosen_opts = group

      # Collect candidate root links from the chosen year ONLY
      root_links: list[str] = []
      if chosen_opts:
        for opt in chosen_opts:
          try:
            await _safe_select(page, opt['value'])
            root_links.extend(await _extract_roots_from_dom(page, page.url, host))
          except Exception as e:
            root_errors.append(f"Failed selecting catalog '{opt}': {e}")
      else:
        # No selectable options; scan current page anchors
        root_links.extend(await _extract_roots_from_dom(page, page.url, host))

      # strict fallback: pick any MC category/tile links when labels are empty (e.g., image-only tiles)
      if not root_links:
        root_links.extend(await _extract_roots_from_dom(page, page.url, host, fallback=True))

      root_links = list(OrderedDict.fromkeys(root_links))

      if not root_links:
        alt = await _extract_roots_from_dom(page, page.url, None, fallback=True)
        root_links = list(OrderedDict.fromkeys(alt))

      # For each root, find schema_url via showCourse / preview_course_nopop.php
      for root_url in root_links:
        try:
          await page.goto(root_url, wait_until='load')
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
              if 'preview_course_nopop.php' in (href or '').lower():
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

# --- PDF Flow
async def process_pdf(
    html: str,
    base_url: str,
    allowed_host: str | None
) -> str:
  soup = BeautifulSoup(html or "", "lxml")
  links = []
  for a in soup.find_all("a", href=True):
    href = a["href"].strip()
    if not href:
      continue
    # Any explicit .pdf OR content URLs that end with .pdf after query removal
    print(f"Checking link {href}")
    candidate = urljoin(base_url, href)
    print(f"After join {href}")
    if ".pdf" not in candidate.lower():
      continue
    # host filter (when we know the school's host)
    if allowed_host:
      try:
        if allowed_host not in urlparse(candidate).netloc:
          continue
      except Exception:
        continue
    # lightweight heuristics to prioritize catalogs
    lower = candidate.lower()
    score = 0
    for kw in ("catalog", "bulletin", "course", "curriculum", "program"):
      if kw in lower:
        score += 1
    # allow everything with .pdf, but sort by score
    links.append((score, candidate))
  # highest scoring first; if ties, keep original order
  links.sort(key=lambda t: (-t[0], t[1]))
  return [c for _, c in links]

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
      if 'catoid' in low:
        # move to front of list
        out.insert(0, u)
      else:
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


async def _smoke_test():
  mc_url = "https://catalog.utah.edu/"  # replace with known Modern Campus URL
  res = await discover_catalog_urls(
    school='University of Utah',
    host='utah.edu',
    presearch_results=[[mc_url]]
  )
  pprint(res)

  close_playwright()

if __name__ == "__main__":
  import asyncio
  asyncio.run(_smoke_test())