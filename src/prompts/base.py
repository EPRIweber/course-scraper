# src/prompts/base.py
from abc import ABC, abstractmethod

class PromptBase(ABC):
    @abstractmethod
    def system(self) -> str: ...
    @abstractmethod
    def user(self)   -> str: ...
