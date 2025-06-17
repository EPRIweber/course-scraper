# src/storage.py
import json
import logging
from pathlib import Path
from abc import ABC, abstractmethod
import re
from typing import List, Dict, Any

# Use relative imports for modules within the same package
from .models import JobSummary

# This regex now has the correct character range '0-9'
_RE_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_RE_UNDERSCORES = re.compile(r"_+")

class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    @abstractmethod
    async def get_urls(self, source: str) -> List[str]: ...
    @abstractmethod
    async def save_urls(self, source: str, urls: List[str]) -> None: ...

    @abstractmethod
    async def get_schema(self, source: str) -> Dict[str, Any]: ...
    @abstractmethod
    async def save_schema(self, source: str, schema: Dict[str, Any]) -> None: ...

    @abstractmethod
    async def get_data(self, source: str) -> List[Dict[str, Any]]: ...
    @abstractmethod
    async def save_data(self, source: str, data: List[Dict[str, Any]]) -> None: ...

    @abstractmethod
    async def save_job_summary(self, summary: JobSummary) -> None:
        """Saves the final job summary."""
        ...

class LocalFileStorage(StorageBackend):
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger(__name__)

    def _ensure_dir(self, source_name: str) -> Path:
        out = self.base_dir / source_name
        out.mkdir(exist_ok=True)
        return out
    
    async def get_urls(self, source_name: str) -> List[str]:
        path = self.base_dir / source_name / "urls.json"
        if not path.exists(): return []
        try:
            with open(path, "r") as f: return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError): return []

    async def save_urls(self, source_name: str, urls: List[str]) -> None:
        d = self._ensure_dir(source_name)
        with open(d / "urls.json", "w") as f: json.dump(list(urls), f, indent=2)

    async def get_schema(self, source_name: str) -> Dict[str, Any]:
        path = self.base_dir / source_name / "schema.json"
        if not path.exists(): return {}
        try:
            with open(path, "r") as f: return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError): return {}

    async def save_schema(self, source_name: str, schema: Dict[str, Any]) -> None:
        d = self._ensure_dir(source_name)
        with open(d / "schema.json", "w") as f: json.dump(schema, f, indent=2)

    async def get_data(self, source_name: str) -> List[Dict[str, Any]]:
        path = self.base_dir / source_name / "courses.json"
        if not path.exists(): return []
        try:
            with open(path, "r") as f: return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError): return []

    async def save_data(self, source_name: str, data: List[Dict[str, Any]]) -> None:
        d = self._ensure_dir(source_name)
        with open(d / "courses.json", "w") as f: json.dump(list(data), f, indent=2)

    async def save_job_summary(self, summary: JobSummary) -> None:
        """Saves the job summary to a file in the base directory."""
        summary_path = self.base_dir / f"run_summary_{summary.job_id}.json"
        summary_dict = summary.model_dump(mode="json")
        with open(summary_path, "w") as f:
            json.dump(summary_dict, f, indent=2)
        self.log.info(f"Saved job summary to {summary_path}")

