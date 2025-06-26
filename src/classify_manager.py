# src/classify_manager.py
from llm_client import GemmaClient
from prompts.base import prompt_registry
import json

def classify_course(title, desc):
    PromptCls = prompt_registry["classify_course"]
    prompt    = PromptCls(title=title, desc=desc)

    client = GemmaClient()
    client.set_response_format({
      "type":"json_object",
      "json_schema":{
        "name":"CourseClassification",
        "schema":{
           "type":"object",
           "properties":{
               "related":{"type":"boolean"},
               "labels" :{"type":"array","items":{"type":"string"}}
           },
           "required":["related","labels"]
        },
        "strict":True
      }
    })

    resp = client.chat([
        {"role":"system","content":prompt.system()},
        {"role":"user",  "content":prompt.user()},
    ], temperature=0.0)

    return json.loads(resp["choices"][0]["message"]["content"])
