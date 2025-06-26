# src/llm_wrapper.py

import json
import requests
from contextlib import contextmanager
from typing import Dict, List, Optional, Any

class EPRI_API:
    """Base class for EPRI API interactions."""
    def __init__(self, api_url: str, model: str, api_key: Optional[str] = None):
        self.api_url = api_url
        self.model   = model
        self.api_key = api_key
        self.response_format: Dict[str,Any] = {}
 
    @contextmanager
    def auth(self):
        headers = {
            "Content-Type": "application/json"
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        try:
            yield headers
        finally:
            pass
    
    def set_response_format(self, fmt: Dict[str,Any]):
        self.response_format = fmt
    
    def chat(self,
             messages: List[Dict[str,str]],
             max_tokens: int = 30000,
             temperature: float = 0.0,
             top_p: Optional[float] = None
    ) -> Dict[str,Any]:
        payload: Dict[str,Any] = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            **({"temperature": temperature} if temperature is not None else {}),
            **({"top_p": top_p} if top_p else {}),
        }
        if self.response_format:
            payload["response_format"] = self.response_format

        print(f"→ {self.api_url}/v1/chat/completions ({self.model})")
        with self.auth_headers() as headers:
            resp = requests.post(f"{self.api_url}/v1/chat/completions",
                                 json=payload, headers=headers)
            resp.raise_for_status()
            return resp.json()



    # def _make_request(self, endpoint: str, payload: Dict, headers: Dict) -> Dict:
    #     print("Making LLM request...")
    #     print(f"- Endpoint: {self.api_url}{endpoint}")
    #     print(f"- Model: {self.model}")
    #     response = requests.post(f"{self.api_url}{endpoint}", json=payload, headers=headers)
    #     if not response.ok:
    #         # DEBUG PRINT
    #         # print("LLM request payload:", json.dumps(payload, indent=2))
    #         print("LLM error response:", response.status_code, response.text)
    #     print(f"Response Complete")
    #     response.raise_for_status()
    #     return response.json()
    
    # def chat_completion(
    #         self,
    #         messages: List[Dict[str, str]],
    #         max_tokens: int = 30000,
    #         temperature: float = None,
    #         top_p: float = None
    # ) -> Dict:
    #     """Generate a chat completion using the specified model and messages."""
    #     payload: Dict[str, Any] = {
    #         "model": self.model,
    #         "messages": messages,
    #         "max_tokens": max_tokens,
    #     }
    #     if top_p:
    #         payload["top_p"] = top_p
    #     if temperature:
    #         payload["temperature"] = temperature
    #     if self.response_format:
    #         payload["response_format"] = self.response_format

    #     with self.auth() as headers:
    #         return self._make_request("/v1/chat/completions", payload, headers)
    
    # def set_response_format(
    #         self,
    #         schema_type: Optional[str] = "json_object",
    #         schema_name: Optional[str] = "CourseExtractionSchema",
    #         schema_description: Optional[str] = "Schema for extracting structured course data from course catalog websites."
    # ):
    #     """
    #     Configuration the response format when extracting repeating data structures,
    #     such as courses, from web sources. Defines the expected output type and schema details
    #     used for structured data extraction.
    #     Params:
    #         schema_type (Optional[str]): The type of the response format, defaulting to "json_object".
    #         schema_name (Optional[str]): The name of the schema used for extraction, defaulting to "CourseExtractionSchema".
    #         schema_description (Optional[str]): A description of the schema, defaulting to "Schema for extracting structured course data from course catalog websites.".
    #     """
    #     self.response_format = {
    #         "type": "json_object",
    #         "json_schema": {
    #             "name": "CourseExtractionSchema",
    #             "description": "CourseExtractionSchema",
    #             "schema": {
    #                 "type": "object",
    #                 "properties": {
    #                     "name":          {"type": "string"},
    #                     "baseSelector":  {"type": "string"},
    #                     "fields": {
    #                         "type":     "array",
    #                         "items":    {"type": "object"}
    #                     }
    #                 },
    #                 "required": ["name", "baseSelector", "fields"]
    #             },
    #             "strict": True
    #         }
    #     }


class GemmaModel(EPRI_API):
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(
            api_url="http://epr-ai-lno-p01.epri.com:8000",
            model="google/gemma-3-27b-it"
        )

class LlamaModel(EPRI_API):
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(
            api_url="http://epr-ai-lno-p01.epri.com:8002",
            model="meta/llama-3.2-90b-vision-instruct"
        )
