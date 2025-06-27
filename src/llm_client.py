# src/llm_client.py
import os
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from openai import OpenAI


class BaseLLMClient:
    """
    Generic LLM client using OpenAI Python SDK. Supports custom API base (e.g., vLLM server) and built-in OpenAI endpoints.
    """
    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
    ):
        self.model = model
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        client_params = {}
        if self.api_key:
            client_params["api_key"] = self.api_key
        if api_base:
            client_params["base_url"] = api_base.rstrip("/")
        self.client = OpenAI(**client_params)
        self.response_format: Dict[str, Any] = {}

    def set_response_format(self, fmt: Dict[str, Any]) -> None:
        """Set the `response_format` payload for structured outputs."""
        self.response_format = fmt

    def chat(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = 30000,
        temperature: float = 0.0,
        top_p: Optional[float] = None,
        stream: bool = False,
    ) -> Any:
        """
        Perform a chat completion request.
        Returns either the full response or a streaming generator if `stream=True`.
        """
        # Build kwargs
        params: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if top_p is not None:
            params["top_p"] = top_p
        if self.response_format:
            params["response_format"] = self.response_format


        # Send request
        completion = self.client.chat.completions.create(
            **params,
            stream=stream
        )
        if stream:
            return completion
        return completion.to_dict()



class GemmaModel(BaseLLMClient):
    """vLLM-based Gemma model client"""

    def __init__(self, api_key: Optional[str] = None):
        super().__init__(
            model="google/gemma-3-27b-it",
            api_key=api_key,
            api_base="http://epr-ai-lno-p01.epri.com:8000/v1"
        )


class LlamaModel(BaseLLMClient):
    """vLLM-based Llama model client"""

    def __init__(self, api_key: Optional[str] = None):
        super().__init__(
            model="meta/llama-3.2-90b-vision-instruct",
            api_key=api_key,
            api_base="http://epr-ai-lno-p01.epri.com:8002/v1"
        )


class ChatGPT(BaseLLMClient):
    """Official OpenAI GPT client"""

    def __init__(self, api_key: Optional[str] = None):
        super().__init__(
            model="gpt-4o-mini",
            api_key=api_key or os.getenv("OPENAI_API_KEY"),
            api_base="https://api.openai.com/v1"
        )
