#./classification/comp_script.py
import os
import pandas as pd
import asyncio
import json
import os
import re
import random
from typing import Dict, Any, List, Tuple
import pyodbc
from openai import AsyncOpenAI
from epri_prompts import FIRST_PASS_SYS_PROMPT, TIER1_SYS_PROMPT, TIER2_SYS_PROMPT

# --- CONFIGURATION ---
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 16))  # Aggressively increased for max batch efficiency on strong local hardware
MAX_PARALLEL = int(os.getenv("MAX_PARALLEL", 16))  # High parallelism to push server utilization
MAX_LLM_RETRIES = int(os.getenv("MAX_LLM_RETRIES", 3))  # Kept for reliable but quick retries
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", 1.5))  # seconds, increased for better backoff under high load
# Throttling / pacing (tune via env vars) - Minimized defaults for maximum throughput on local server
ROWS_LIMIT = int(os.getenv("ROWS_LIMIT", "0"))  # 0 = no limit; set to e.g. 500 for testing large pushes
SLEEP_BETWEEN_BATCHES = float(os.getenv("SLEEP_BETWEEN_BATCHES", "0.1"))  # Minimal delay to keep pace high
BATCH_SLEEP_JITTER = float(os.getenv("BATCH_SLEEP_JITTER", "0.1"))  # Light jitter to prevent overload spikes
PER_REQUEST_DELAY = float(os.getenv("PER_REQUEST_DELAY", "0.1"))  # Very low for aggressive local processing
PER_REQUEST_JITTER = float(os.getenv("PER_REQUEST_JITTER", "0.05"))  # Minimal jitter for stability
# --- CONNECTION STRINGS & ENDPOINTS ---
def _brace_pwd(v: str) -> str:
    v = v or ""
    return "{" + v.replace("}", "}}") + "}"

CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};PWD={_brace_pwd(os.getenv('DB_PASS'))};"
    "Encrypt=yes;TrustServerCertificate=yes;"
)

API_KEY: str = os.getenv("OPENAI_API_KEY", "")
BASE_URL = os.getenv("OPENAI_BASE_URL", "http://epr-ai-lno-p01.epri.com:8002/v1")
MODEL = os.getenv("OPENAI_MODEL", "meta/llama-3.2-90b-vision-instruct")

UNIVERSITIES = ['texas_a_and_m_university']

# --- UTILITIES ---
FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)\s*```", re.I)

def strip_fence(txt) -> str:
    if txt is None:
        return ""
    s = txt if isinstance(txt, str) else str(txt)
    s = s.strip()
    m = FENCE_RE.search(s)
    return m.group(1).strip() if m else s.strip("` \n")

def extract_json_from_response(resp) -> Tuple[str, Dict[str, Any]]:
    content = ""
    try:
        choices = getattr(resp, "choices", []) or []
        msg = choices[0].message if choices else None
        raw = getattr(msg, "content", None) if msg else None
        if raw is None and msg is not None:
            parsed = getattr(msg, "parsed", None)
            if parsed is not None:
                raw = parsed if isinstance(parsed, str) else json.dumps(parsed)
        content = strip_fence(raw)
        if not content:
            raise ValueError("empty content")
        try:
            return content, json.loads(content)
        except json.JSONDecodeError:
            m = re.search(r"\{[\s\S]*\}", content)
            if m:
                return content, json.loads(m.group(0))
            raise
    except Exception as e:
        preview = (content or "")[:200].replace("\n", " ")
        raise RuntimeError(f"Could not extract JSON; preview={preview!r}; err={e}") from e

async def rate_limit_pause():
    """Small per-request pause to avoid thundering herds."""
    if PER_REQUEST_DELAY > 0 or PER_REQUEST_JITTER > 0:
        import random as _rnd
        delay = PER_REQUEST_DELAY + _rnd.random() * PER_REQUEST_JITTER
        try:
            await asyncio.sleep(delay)
        except Exception:
            pass

# Token accounting
total_prompt_tokens = 0
total_completion_tokens = 0
total_tokens = 0

def _accumulate_usage(usage) -> None:
    global total_prompt_tokens, total_completion_tokens, total_tokens
    if not usage:
        return
    total_prompt_tokens += getattr(usage, "prompt_tokens", 0) or 0
    total_completion_tokens += getattr(usage, "completion_tokens", 0) or 0
    total_tokens += getattr(usage, "total_tokens", 0) or 0

# Retrying chat helper
async def safe_chat_json(
    client: AsyncOpenAI,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 600,
    temperature: float = 0.0,
    require_json: bool = True,
) -> Dict[str, Any]:
    last_exc = None
    for attempt in range(MAX_LLM_RETRIES):
        try:
            use_resp_fmt = require_json and (attempt < MAX_LLM_RETRIES - 1)
            response = await client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=max_tokens,
                temperature=temperature,
                **({"response_format": {"type": "json_object"}} if use_resp_fmt else {}),
            )
            _accumulate_usage(getattr(response, "usage", None))
            _content, j = extract_json_from_response(response)
            return j
        except Exception as e:
            last_exc = e
            if attempt < MAX_LLM_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt) + random.random() * 0.25
                await asyncio.sleep(delay)
                continue
            raise
    raise last_exc or RuntimeError("Unknown failure in safe_chat_json")

async def gather_in_batches(
    coroutines: List[asyncio.Task], batch_size: int, total_items: int, pass_name: str
) -> List:
    results: List = []
    if not coroutines:
        return results
    for i in range(0, len(coroutines), batch_size):
        batch = coroutines[i : i + batch_size]
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
        print(f"  → Pass '{pass_name}': {len(results)}/{total_items} courses processed...", end="\r")
        # Gentle pause between batches if requested
        if SLEEP_BETWEEN_BATCHES > 0:
            delay = SLEEP_BETWEEN_BATCHES + random.random() * BATCH_SLEEP_JITTER
            await asyncio.sleep(delay)
    print()
    return results

# --- PIPELINE ---
async def process_university(cx: pyodbc.Connection, uni: str, client: AsyncOpenAI) -> None:
    global total_prompt_tokens, total_completion_tokens, total_tokens

    try:
        print(f"\n📚 Processing {uni}...")
        cur = cx.cursor()
        cur.execute(
            "SELECT course_id, course_title, course_description "
            "FROM dbo.courses WHERE course_source_id = "
            f"(SELECT TOP 1 source_id FROM dbo.sources WHERE source_name = ?)",
            (uni,),
        )
        rows = cur.fetchall()
        # Optional: limit number of rows for low-load runs
        if ROWS_LIMIT > 0:
            rows = rows[:ROWS_LIMIT]
    except Exception as e:
        print(f"❌ Database query failed for {uni}: {e}")
        return

    total_courses = len(rows)
    print(f"📚 Found {total_courses} courses for {uni}.")
    if not rows:
        return

    sem = asyncio.Semaphore(MAX_PARALLEL)

    # PASS 1 – Hydrogen-Relevant (Tier 1) gate
    async def run_first_pass(record):
        cid, title, desc = record
        clean_title = ("" if title is None else str(title)).replace("\n", " ").replace("\r", " ")
        clean_desc = ("" if desc is None else str(desc)).replace("\n", " ").replace("\r", " ")
        async with sem:
            await rate_limit_pause()
            try:
                j = await safe_chat_json(
                    client,
                    FIRST_PASS_SYS_PROMPT,
                    f"Title: {clean_title}\n\nDescription:\n{clean_desc}",
                    max_tokens=200,
                    temperature=0.0,
                )
                tiers = j.get("tiers", [0])
                if isinstance(tiers, list) and 2 in tiers:
                    tiers = [1]
                return {"cid": cid, "title": title, "desc": desc, "tiers": tiers}
            except Exception as e:
                print(f"\n⚠️ First Pass Error for course {cid} ({uni}): {e}")
                return {"cid": cid, "title": title, "desc": desc, "error": f"pass1_error:{e}"}

    print(f"🚀 Starting Pass 1 (Hydrogen Relevant gate) for {uni}...")
    first_pass_results = await gather_in_batches([run_first_pass(r) for r in rows], BATCH_SIZE, len(rows), f"1-{uni}")
    print(f"✅ Pass 1 Complete for {uni}.")

    # Filter Tier 1
    tier1_courses = [r for r in first_pass_results if r and ("error" not in r) and (1 in (r.get("tiers") or []))]

    tier1_map: Dict[Any, Tuple[List[str], str, str]] = {}
    tier2_map: Dict[Any, Dict[str, str]] = {}

    # PASS 2A – Tier 1 labels (A–I)
    if tier1_courses:
        async def run_second_pass_tier1(record):
            clean_title = ("" if record.get("title") is None else str(record["title"]))\
                .replace("\n", " ").replace("\r", " ")
            clean_desc = ("" if record.get("desc") is None else str(record["desc"]))\
                .replace("\n", " ").replace("\r", " ")
            async with sem:
                await rate_limit_pause()
                try:
                    j = await safe_chat_json(
                        client,
                        TIER1_SYS_PROMPT,
                        f"Title: {clean_title}\n\nDescription:\n{clean_desc}",
                        max_tokens=1000,
                        temperature=0.0,
                    )
                    return {
                        "cid": record["cid"],
                        "labels": j.get("tier1_labels", []),
                        "why": j.get("why_tier1", ""),
                        "unsure": j.get("unsure", ""),
                    }
                except Exception as e:
                    return {"cid": record["cid"], "error": f"pass2_t1_error:{e}"}

        print(f"🚀 Starting Pass 2A: Tier 1 labels (A–I) for {len(tier1_courses)} courses in {uni}...")
        t1_results = await gather_in_batches([run_second_pass_tier1(r) for r in tier1_courses], BATCH_SIZE, len(tier1_courses), f"2A-Tier1-{uni}")
        tier1_map = {
            r["cid"]: (r.get("labels", []), r.get("why", ""), r.get("unsure", ""))
            for r in t1_results if r and "error" not in r
        }
        print(f"✅ Pass 2A (Tier 1) Complete for {uni}.")

    # PASS 2B – Hydrogen Specific
    if tier1_courses:
        async def run_second_pass_tier2(record):
            clean_title = ("" if record.get("title") is None else str(record["title"]))\
                .replace("\n", " ").replace("\r", " ")
            clean_desc = ("" if record.get("desc") is None else str(record["desc"]))\
                .replace("\n", " ").replace("\r", " ")
            async with sem:
                await rate_limit_pause()
                try:
                    j = await safe_chat_json(
                        client,
                        TIER2_SYS_PROMPT,
                        f"Title: {clean_title}\n\nDescription:\n{clean_desc}",
                        max_tokens=900,
                        temperature=0.0,
                    )
                    hs = j.get("hydrogen_specific", [])
                    topic = j.get("topic", "")
                    why = j.get("why_tier2", "")
                    return {
                        "cid": record["cid"],
                        "hydrogen_specific": hs if isinstance(hs, list) else [hs],
                        "topic": topic,
                        "why": why,
                    }
                except Exception as e:
                    return {"cid": record["cid"], "error": f"pass2_t2_error:{e}"}

        print(f"🚀 Starting Pass 2B: Tier 2 (Hydrogen Specific) for {len(tier1_courses)} courses in {uni}...")
        t2_results = await gather_in_batches([run_second_pass_tier2(r) for r in tier1_courses], BATCH_SIZE, len(tier1_courses), f"2B-Tier2-{uni}")
        tier2_map = {
            r["cid"]: {
                "hydrogen_specific": r.get("hydrogen_specific", []),
                "topic": r.get("topic", ""),
                "why": r.get("why", ""),
            }
            for r in t2_results if r and "error" not in r
        }
        print(f"✅ Pass 2B (Tier 2) Complete for {uni}.")

    # MERGE + SAVE
    print(f"✍️  Merging results for {uni}...")
    final_rows = []
    for res in first_pass_results:
        if not res:
            continue
        cid = res["cid"]
        t1_labels, why_t1, unsure_t1 = tier1_map.get(cid, ([], "", ""))
        t2 = tier2_map.get(cid, {"hydrogen_specific": [], "topic": "", "why": ""})

        row = {
            "course_id": cid,
            "course_title": res.get("title"),
            "description": res.get("desc"),
            "tier": ",".join(map(str, sorted(res.get("tiers", [])))) if "error" not in res else "ERROR",
            "tier1_labels": ",".join(map(str, sorted(t1_labels))),
            "why_tier1": why_t1,
            "tier1_unsure": unsure_t1,
            "hydrogen_specific": ",".join(t2.get("hydrogen_specific", [])),
            "tier2_topic": t2.get("topic", ""),
            "why_tier2": t2.get("why", ""),
        }
        final_rows.append(row)

    if not final_rows:
        print(f"No results to write for {uni}. Skipping.")
        return

    output_df = pd.DataFrame(final_rows)
    output_file_path = f"{uni}_tiered_taxonomy_twopass.xlsx"
    try:
        output_df.to_excel(output_file_path, index=False, engine='openpyxl')
        print(f"💾 Saved → {output_file_path}")
        print(f"✨ Processing complete for {uni}!")
    except Exception as e:
        print(f"❌ Could not save as Excel file for {uni} (Error: {e}). Falling back to CSV.")
        csv_file_path = f"{uni}_tiered_taxonomy_twopass.csv"
        try:
            output_df.to_csv(csv_file_path, index=False)
            print(f"💾 Saved as CSV → {csv_file_path}")
            print(f"✨ Processing complete for {uni}! (Note: Install 'openpyxl' via `pip install openpyxl` for Excel support.)")
        except Exception as csv_e:
            print(f"❌ Could not save as CSV either for {uni}. Error: {csv_e}")

# --- MAIN ---
async def main() -> None:
    global total_prompt_tokens, total_completion_tokens, total_tokens
    try:
        print("Connecting to database...")
        with pyodbc.connect(CONN_STR) as cx:
            client = AsyncOpenAI(api_key=API_KEY, base_url=BASE_URL, timeout=120, max_retries=0)
            for uni in UNIVERSITIES:
                await process_university(cx, uni, client)
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
    finally:
        print("\n📊 Token Usage Summary:")
        print(f"  Total Input Tokens (Prompt): {total_prompt_tokens}")
        print(f"  Total Output Tokens (Completion): {total_completion_tokens}")
        print(f"  Grand Total Tokens: {total_tokens}")

if __name__ == "__main__":
    if not all(os.getenv(var) for var in ['DB_SERVER', 'DB_NAME', 'DB_USER', 'DB_PASS']):
        print("FATAL: One or more database environment variables (DB_SERVER, DB_NAME, DB_USER, DB_PASS) are not set.")
    else:
        asyncio.run(main())