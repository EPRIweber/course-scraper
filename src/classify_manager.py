# src/classify_manager.py
import os
import asyncio
import csv
import json
from typing import List, Tuple, Dict, TypedDict
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

async def classify_courses(
    courses: List[Tuple[str, str, str]]
) -> Tuple[List[Tuple[str, List[str]]], int]:
    """
    Performs two-pass classification: first top-level, then subtree classification.
    Returns list of (course_id, combined labels) and total token usage.
    """
    # courses = courses[0:200]
    # Build maps for easy lookup
    title_map: Dict[str,str] = {cid: title for cid, title, _ in courses}
    desc_map:  Dict[str,str] = {cid: desc  for cid, _, desc in courses}

    api_key  = os.getenv("OPENAI_API_KEY")
    base_url = "http://epr-ai-lno-p01.epri.com:8000/v1"
    model    = "google/gemma-3-27b-it"

    async_client = AsyncOpenAI(api_key=api_key, base_url=base_url)

    # --- First pass: top-level classes ---
    msgs1 = [
        [
            {"role":"system","content":taxonomy_sys_prompt},
            {"role":"user","content":f"## Title:\n{title_map[cid]}\n\n## Description:\n{desc_map[cid]}"}
        ]
        for cid, _, _ in courses
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
    responses1 = await asyncio.gather(*tasks1)

    primary: List[Tuple[str,List[str]]] = []
    usage1 = 0
    for (cid, _, _), resp in zip(courses, responses1):
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
                f"Here are subclass options under ID(s) {', '.join(labels)}:\n\n"
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







# import os
# import asyncio
# from typing import List, Tuple, TypedDict
# from src.prompts.taxonomy import format_subtree, load_full_taxonomy, taxonomy_sys_prompt
# # system
# from datetime import datetime, timezone
# from typing import Unpack

# # package
# import datetime as dt
# from openai import AsyncOpenAI
# from openai.types.chat.completion_create_params import CompletionCreateParamsNonStreaming
# from sigfig import round


# class CompletionResult(TypedDict):
#     '''EPRI-customized typed dictionary representing a chat completion result'''

#     created_utc: dt.datetime
#     '''Timestamp (UTC) when the chat completion was created, as reported by the server hosting the model'''
    
#     user_message: str | None
#     '''User's message (last message sent into the model)'''

#     completion_text: str
#     '''Model's generated text with leading/trailing whitespaces removed'''

#     prompt_tokens: int | None
#     '''Number of tokens in the prompt'''

#     completion_tokens: int | None
#     '''Number of tokens in the completion text'''

#     total_tokens: int | None
#     '''Total number of tokens processed (prompt + completion)'''

#     elapsed_sec: float
#     '''Elapsed time (seconds) while the chat completion request was being processed'''

#     tokens_per_sec: float | None
#     '''Total tokens divided by elapsed time'''

# async def _get_chat_completion_async(async_client: AsyncOpenAI, **kwargs: Unpack[CompletionCreateParamsNonStreaming]) -> CompletionResult:
#     '''
#     Asynchronous chat completion that returns the generated text and usage
#     stats in a custom-defined CompletionResult dict.

#     Parameters
#     ----------
#     async_client : AsyncOpenAI
#         An asynchronous OpenAI client instance, usually instantiated by calling
#         `AsyncOpenAI(api_key=api_key, base_url=base_url)`. You can (and should)
#         reuse the same async client instance for efficiency, especially with
#         concurrency (sending multiple concurrent chat completion requests).

#     **kwargs : CompletionCreateParamsNonStreaming
#         Any "client.chat.completions.create()" keyword arguments, such as
#         `messages` and `model` to pass into the create() function.
#     '''
    
#     # Capture timestamp just before requesting a completion
#     tic = datetime.now()

#     # Request a completion for the given chat messages
#     response = await async_client.chat.completions.create(**kwargs)

#     # Capture timestamp just after receiving the completion
#     toc = datetime.now()

#     # Get the last message, which we assume is the user's last message sent
#     user_message = list(kwargs['messages'])[-1].get('content')
    
#     # Ensure the user message is a string (or stringified) if it exists
#     if user_message is not None:
#         user_message = str(user_message)
        

#     # Get the model's generated text, removing any leading/trailing whitespaces
#     completion_text = (response.choices[0].message.content or '').strip()

#     # Get total token count, or None if usage stats are not enabled
#     total_tokens = response.usage.total_tokens if response.usage else None

#     # Get elapsed time (seconds) with 3 significant digits
#     elapsed_sec = float(round((toc - tic).total_seconds(), sigfigs=3))

#     # Get throughput (tokens per second) with 3 significant digits
#     tokens_per_sec = float(round(total_tokens / elapsed_sec, sigfigs=3)) if total_tokens else None

#     # Return a CompletionResult dict
#     return {
#         "created_utc": datetime.fromtimestamp(response.created, tz=timezone.utc),
#         "user_message": user_message,
#         "completion_text": completion_text,
#         "prompt_tokens": response.usage.prompt_tokens if response.usage else None,
#         "completion_tokens": response.usage.completion_tokens if response.usage else None,
#         "total_tokens": total_tokens,
#         "elapsed_sec": elapsed_sec,
#         "tokens_per_sec": tokens_per_sec
#     }

# def _prepare_messages(courses: List[Tuple[str, str, str]]) -> List[Tuple[str, List[dict]]]:
#     """
#     Convert list of (course_id, title, desc) into list of (course_id, messages) for async calls
#     """
#     msgs = []
#     # for course_id, title, desc in courses:
#     for course_id, title, desc in courses[0:200]:
#         messages = [
#             {"role": "system", "content": taxonomy_sys_prompt},
#             {"role": "user",   "content": f"## Title:\n{title}\n\n## Description:\n{desc}"}
#         ]
#         msgs.append((course_id, messages))
#     return msgs


# async def classify_courses(
#     courses: List[Tuple[str, str, str]]
# ) -> Tuple[List[Tuple[str, List[str]]], int]:
#     """
#     Classify courses concurrently via Gemma model, returning list of
#     (course_id, [labels]) and total tokens used.
#     """
#     # Configuration from env (or hardcode as in GemmaModel)
#     api_key  = os.getenv("OPENAI_API_KEY")
#     base_url = "http://epr-ai-lno-p01.epri.com:8000/v1"
#     model    = "google/gemma-3-27b-it"

#     # Prepare messages for each course
#     messages_list = _prepare_messages(courses)

#     # Initialize async client
#     async_client = AsyncOpenAI(api_key=api_key, base_url=base_url)

#     # Create tasks for all courses
#     tasks = [
#         _get_chat_completion_async(
#             async_client,
#             model=model,
#             messages=msgs,
#             max_tokens=30000,
#             temperature=0.0
#         )
#         for _, msgs in messages_list
#     ]

#     # Await all concurrently
#     responses: List[CompletionResult] = await asyncio.gather(*tasks)

#     results = []
#     total_usage = 0
#     # Parse each response
#     for (course_id, _), resp in zip(messages_list, responses):
#         content = resp.get("completion_text", "")
#         usage = resp.get("total_tokens", 0) or 0

#         # Split into label list
#         labels = [lbl.strip() for lbl in content.replace("\n", ",").split(",") if lbl.strip()]
#         results.append((course_id, labels))
#         total_usage += usage
    
#     followup_tasks = []
#     taxonomy = load_full_taxonomy()
#     for course_id, labels in results:
#         if not labels:
#             continue

#         subtree_md = format_subtree(labels, taxonomy)

#         prompt = [
#         {"role":"system", "content": taxonomy_sys_prompt},
#         {"role":"user",   "content": f"""
# Here are the subclass options under ID(s) {', '.join(labels)}:

# {subtree_md}

# ## Course to classify
# Title: {title_map[course_id]}
# Description: {desc_map[course_id]}

# **Instruction**: From the options above, select all subclass IDs that apply. 
# Respond *only* with comma-separated subclass IDs (e.g. `1.1.2,1.3.1.1`).
# """}
#     ]
#         followup_tasks.append((course_id, prompt))


#     return results, total_usage
