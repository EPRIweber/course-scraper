# src/llm_client.py
import os, requests, openai
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
    def auth_headers(self):
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

class ChatGPT(EPRI_API):
    def __init__(self, api_key: Optional[str] = None):
        if not api_key:
            api_key=os.getenv("OPENAI_API_KEY")
        super().__init__(
            api_key=api_key,
            model="gpt-4o-mini",
            api_url="https://api.openai.com"
        )
    
    def chat(self,
             messages: List[Dict[str,str]],
             max_tokens: int = 16384,
             temperature: float = 0.0,
             top_p: Optional[float] = None
    ) -> Dict[str,Any]:
        client = openai.OpenAI(api_key=self.api_key)
        response = client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens
        )
        return response