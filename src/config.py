# src/config.py
from pydantic import BaseModel, HttpUrl, Extra
from typing import List, Optional
import yaml
from pathlib import Path

class SourceConfig(BaseModel):
    """
    Configuration for a single data source (school).
    """
    name: str
    root_url: HttpUrl
    schema_url: HttpUrl
    include_external: Optional[bool] = None
    crawl_depth: Optional[int] = 3
    page_timeout_s: Optional[int] = 10
    word_count_min: Optional[int] = None
    query: Optional[str] = None
    max_concurrency: Optional[int] = 10  # Default concurrency for tasks

    class Config:
        extra = 'forbid'

class AppConfig(BaseModel):
    """
    Top-level application configuration, holds all sources.
    """
    sources: List[SourceConfig]

    class Config:
        extra = 'forbid'

# Load sources from the YAML file at the project root.
# This makes the path relative to this file's location.
try:
    config_path = Path(__file__).parent.parent / "configs/sources.yaml"
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)
    config = AppConfig(**raw)
except FileNotFoundError:
    raise FileNotFoundError(f"Configuration file not found at {config_path}. Make sure sources.yaml is in the project root.")
except Exception as e:
    raise ValueError(f"Error loading or parsing sources.yaml: {e}")

