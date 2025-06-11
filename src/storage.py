# src/storage.py
import json
from pathlib import Path
from typing import List, Dict, Any

class LocalFileStorage:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _ensure_dir(self, source_name: str) -> Path:
        out = self.base_dir / source_name
        out.mkdir(exist_ok=True)
        return out
    
    def get_urls(self, source_name: str) -> List[str]:
        d = self.base_dir / source_name
        if not d.exists():
            return []
        try:
            with open(d / "urls.json", "r") as f:
                return json.load(f)
        except:
            return []

    def save_urls(self, source_name: str, urls: List[str]) -> None:
        d = self._ensure_dir(source_name)
        with open(d / "urls.json", "w") as f:
            json.dump(list(urls), f, indent=2)

    def save_schema(self, source_name: str, schema: Dict[str, Any]) -> None:
        # if you ever want to override cache on disk
        d = self._ensure_dir(source_name)
        with open(d / "schema.json", "w") as f:
            json.dump(schema, f, indent=2)

    def get_schema(self, source_name: str) -> Dict[str, Any]:
        d = self.base_dir / source_name
        if not d.exists():
            return {}
        try:
            with open(d / "schema.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def save_data(self, source_name: str, data: List[Dict[str, Any]]) -> None:
        d = self._ensure_dir(source_name)
        with open(d / "courses.json", "w") as f:
            json.dump(list(data), f, indent=2)

    def get_data(self, source_name: str) -> List[Dict[str, Any]]:
        d = self.base_dir / source_name
        if not d.exists():
            return []
        try:
            with open(d / "courses.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return []