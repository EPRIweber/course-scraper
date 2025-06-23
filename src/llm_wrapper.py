# src/llm_wrapper.py

import json
import requests
from contextlib import contextmanager
from typing import Dict, List, Optional, Any

class EPRI_API:
    """Base class for EPRI API interactions."""
    def __init__(self, api_url: str, api_key: Optional[str] = None):
        self.api_url = api_url
        self.api_key = api_key

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

    def _make_request(self, endpoint: str, payload: Dict, headers: Dict) -> Dict:
        print("Making LLM request...")
        print(f"- Endpoint: {self.api_url}{endpoint}")
        print(f"- Model: {payload["model"]}")
        response = requests.post(f"{self.api_url}{endpoint}", json=payload, headers=headers)
        if not response.ok:
            # DEBUG PRINT
            # print("LLM request payload:", json.dumps(payload, indent=2))
            print("LLM error response:", response.status_code, response.text)
        print(f"Response Complete")
        response.raise_for_status()
        return response.json()
    
    def chat_completion(
            self,
            model: str,
            messages: List[Dict[str, str]],
            max_tokens: int = 10000,
            temperature: float = None,
            top_p: float = None,
            response_format: Optional[Dict[str, Any]] = None,
    ) -> Dict:
        """Generate a chat completion using the specified model and messages."""
        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
        }
        if top_p:
            payload["top_p"] = top_p
        if temperature:
            payload["temperature"] = temperature
        if response_format:
            payload["response_format"] = response_format

        with self.auth() as headers:
            return self._make_request("/v1/chat/completions", payload, headers)


class GemmaModel(EPRI_API):
    def tokenize(self, text: str) -> List[str]:
        """Tokenize the given text."""
        with self.auth() as headers:
            payload = {"text": text}
            return self._make_request("/tokenize", payload, headers)["tokens"]


class LlamaModel(EPRI_API):
    pass