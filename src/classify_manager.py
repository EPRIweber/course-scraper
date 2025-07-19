# src/classify_manager.py
import os
import asyncio
import csv
import json
from typing import Any, List, Tuple, Dict, TypedDict
from openai import AsyncOpenAI
from openai.types.chat.completion_create_params import CompletionCreateParamsNonStreaming
from sigfig import round
from datetime import datetime, timezone
import datetime as dt

from src.prompts.taxonomy import (
    taxonomy_sys_prompt,
    load_full_taxonomy,
    format_subtree,
)



async def classify_courses(
    courses: List[Tuple[str, str, str]]
) -> Tuple[List[Tuple[str, List[str]]], int]:
    """
    Performs two-pass classification: first top-level, then subtree classification.
    Returns list of (course_id, combined labels) and total token usage.
    """
    batch_size=300
    batch_index=1
    usage1 = 0
    title_map = {cid: title for cid, title, _ in courses}
    desc_map  = {cid: desc  for cid, _, desc in courses}
    primary: List[Tuple[str,List[str]]] = []
    
    api_key  = os.getenv("OPENAI_API_KEY")
    base_url = "http://epr-ai-lno-p01.epri.com:8000/v1"
    async_client   = AsyncOpenAI(api_key=api_key, base_url=base_url)
    model    = "google/gemma-3-27b-it"

    for batch in (courses[i:i+batch_size] 
                  for i in range(0, len(courses), batch_size)):
        # courses = courses[0:200]
        # Build maps for easy lookup

        # api_key  = os.getenv("OPENAI_API_KEY")
        # base_url = "http://epr-ai-lno-p01.epri.com:8000/v1"
        # model    = "google/gemma-3-27b-it"

        # async_client = AsyncOpenAI(api_key=api_key, base_url=base_url)

        # --- First pass: top-level classes ---
        msgs1 = [
            [
                {"role":"system","content":taxonomy_sys_prompt},
                {"role":"user","content":f"## Title:\n{title_map[cid]}\n\n## Description:\n{desc_map[cid]}"}
            ]
            for cid, _, _ in batch
        ]
        tasks1 = [
            _get_chat_completion_async(
                async_client,
                model=model,
                messages=msgs,
                max_tokens=30000,
                temperature=0.0
            )
            for msgs in msgs1
        ]
        print(f"Gathering batch {batch_index} of {len(courses) / batch_size}...")
        responses1 = await asyncio.gather(*tasks1)
        print(f"Batch {batch_index} Complete")

        batch_index+=1
        
        for (cid, _, _), resp in zip(batch, responses1):
            labels = [lbl.strip() for lbl in resp['completion_text'].replace("\n",",").split(",") if lbl.strip()]
            primary.append((cid, labels))
            usage1 += resp.get('total_tokens', 0) or 0

    # --- Second pass: subtree classification ---
    taxonomy = load_full_taxonomy()
    followup_tasks = []
    ids_for_task: List[str] = []
    for cid, labels in primary:
        if not labels:
            continue
        subtree_md = format_subtree(labels, taxonomy)
        prompt = [
            {"role":"system","content":taxonomy_sys_prompt},
            {"role":"user","content":(
                f"**Second Step:**\n\nHere are subclass options under ID(s) {', '.join(labels)}:\n\n"
                f"{subtree_md}\n\n"
                f"## Course to classify\n"
                f"Title: {title_map[cid]}\n"
                f"Description: {desc_map[cid]}\n\n"
                "Instruction: Respond only with comma-separated subclass IDs."
            )}
        ]
        followup_tasks.append(
            _get_chat_completion_async(
                async_client,
                model=model,
                messages=prompt,
                max_tokens=30000,
                temperature=0.0
            )
        )
        ids_for_task.append(cid)

    responses2 = await asyncio.gather(*followup_tasks) if followup_tasks else []
    usage2 = sum(resp.get('total_tokens', 0) or 0 for resp in responses2)

    # Combine primary + sub-labels
    final: List[Tuple[str,List[str]]] = []
    for cid, labels in primary:
        if cid in ids_for_task:
            idx = ids_for_task.index(cid)
            sub_labels = [lbl.strip() for lbl in responses2[idx]['completion_text'].split(',') if lbl.strip()]
            combined = labels + sub_labels
        else:
            combined = labels
        final.append((cid, combined))

    return final, usage1 + usage2

class CompletionResult(TypedDict):
    """EPRI-customized typed dictionary representing a chat completion result"""
    created_utc: dt.datetime
    user_message: str | None
    completion_text: str
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    elapsed_sec: float
    tokens_per_sec: float | None

async def _get_chat_completion_async(
    async_client: AsyncOpenAI,
    **kwargs: CompletionCreateParamsNonStreaming
) -> CompletionResult:
    tic = datetime.now()
    response = await async_client.chat.completions.create(**kwargs)
    toc = datetime.now()

    user_message = list(kwargs['messages'])[-1].get('content')
    if user_message is not None:
        user_message = str(user_message)

    completion_text = (response.choices[0].message.content or '').strip()
    total_tokens = response.usage.total_tokens if response.usage else None
    elapsed_sec = float(round((toc - tic).total_seconds(), sigfigs=3))
    tokens_per_sec = float(round(total_tokens / elapsed_sec, sigfigs=3)) if total_tokens else None

    return {
        "created_utc": datetime.fromtimestamp(response.created, tz=timezone.utc),
        "user_message": user_message,
        "completion_text": completion_text,
        "prompt_tokens": response.usage.prompt_tokens if response.usage else None,
        "completion_tokens": response.usage.completion_tokens if response.usage else None,
        "total_tokens": total_tokens,
        "elapsed_sec": elapsed_sec,
        "tokens_per_sec": tokens_per_sec
    }

def flatten_taxonomy(tree: dict[str,Any], prefix: str = "") -> set[str]:
    """
    Walks your nested taxonomy JSON and returns a set of every full ID.
    E.g. {"1": {"_description":..., "1": {...}, "2": {...}}, ...}
    yields {"1","1.1","1.1.1","1.1.2",...}
    """
    ids = set()
    for key, node in tree.items():
        if key == "_description":
            continue
        full = f"{prefix}.{key}" if prefix else key
        ids.add(full)
        # recurse into children
        ids |= flatten_taxonomy(node, full)
    return ids
