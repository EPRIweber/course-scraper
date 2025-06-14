# src/config.py
from pydantic import BaseModel, HttpUrl, Extra
from typing import List
import yaml

class SourceConfig(BaseModel):
    name: str
    root_url: HttpUrl
    schema_url: HttpUrl
    include_external: bool = None # type: ignore
    crawl_depth: int = None # type: ignore
    page_timeout_s: int = None # type: ignore
    word_count_min: int = None # type: ignore
    query: str = None # type: ignore
    max_concurrency: int = None # type: ignore

    class Config:
        extra = 'forbid'

class AppConfig(BaseModel):
    sources: List[SourceConfig]

    class Config:
        extra = 'forbid'

raw = yaml.safe_load(open("configs/sources.yaml"))
config = AppConfig(**raw)
