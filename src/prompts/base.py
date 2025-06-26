# src/prompts/base.py
from abc import ABC, abstractmethod
prompt_registry: dict[str, type] = {}

class PromptBase(ABC):
    @abstractmethod
    def system(self) -> str: ...
    @abstractmethod
    def user(self)   -> str: ...

def register(name: str):
    def deco(cls):
        prompt_registry[name] = cls
        return cls
    return deco
