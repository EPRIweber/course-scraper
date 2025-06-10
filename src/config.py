# src/config.py
from pydantic import BaseModel, HttpUrl, Extra
from typing import List
import yaml

class SourceConfig(BaseModel):
    name: str
    root_url: HttpUrl
    schema_url: HttpUrl
    include_external: bool
    crawl_depth: int = 5
    page_timeout_ms: int = 5000

    class Config:
        extra = 'forbid'

class AppConfig(BaseModel):
    sources: List[SourceConfig]

    class Config:
        extra = 'forbid'

raw = yaml.safe_load(open("configs/sources.yaml"))
config = AppConfig(**raw)
