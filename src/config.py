# src/config.py
from pydantic import BaseModel, HttpUrl, Extra
from typing import List
import yaml

class SourceConfig(BaseModel):
    name: str
    root_url: HttpUrl
    schema_url: HttpUrl
    include_external: bool = None
    crawl_depth: int = None
    page_timeout_ms: int = None
    word_count_min: int = None

    class Config:
        extra = 'forbid'

class AppConfig(BaseModel):
    sources: List[SourceConfig]

    class Config:
        extra = 'forbid'

raw = yaml.safe_load(open("configs/sources.yaml"))
config = AppConfig(**raw)
