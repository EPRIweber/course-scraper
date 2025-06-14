# src/storage.py
import json
import logging
from pathlib import Path
from abc import ABC, abstractmethod
import re
from typing import List, Dict, Any
from google.cloud import firestore

_RE_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_RE_UNDERSCORES = re.compile(r"_+")

class StorageBackend(ABC):
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

class FirestoreStorage(StorageBackend):
    def __init__(self, project=None):
        self.db = firestore.AsyncClient(project=project)
        self.root = self.db.collection("scraper")
        self.log = logging.getLogger(__name__)
    
    def _slugify(self, s: str) -> str:
        # Lowercase, replace non-alnum with underscore, collapse multiple underscores
        s = s.lower()
        s = _RE_NON_ALNUM.sub("_", s)
        s = _RE_UNDERSCORES.sub("_", s).strip("_")
        return s or "no_title"

    async def get_urls(self, source_name: str) -> List[str]:
        doc = self.root.document(source_name).collection("urls").document("list")
        snap = await doc.get()
        self.log.info(f"Loaded URLs for {source_name}")
        return (snap.to_dict() or {}).get("items", [])

    async def save_urls(self, source_name: str, urls: List[str]) -> None:
        doc = self.root.document(source_name).collection("urls").document("list")
        self.log.info(f"Saving {len(urls)} URLs to {source_name}")
        await doc.set({"items": urls})

    async def get_schema(self, source_name: str) -> Dict[str, Any]:
        doc = self.root.document(source_name).collection("schema").document("definition")
        snap = await doc.get()
        self.log.info(f"Loaded schema for {source_name}")
        return snap.to_dict() or {}

    async def save_schema(self, source_name: str, schema: Dict[str, Any]) -> None:
        doc = self.root.document(source_name).collection("schema").document("definition")
        await doc.set(schema)
        self.log.info(f"Saved schema to {source_name}")

    async def get_data(self, source_name: str) -> List[Dict[str, Any]]:
        col = self.root.document(source_name).collection("courses")
        snapshots = await col.get()
        self.log.info(f"Loaded {len(snapshots)} course documents for {source_name}")
        return [doc.to_dict() for doc in snapshots]

    async def save_data(self, source_name: str, data: List[Dict[str, Any]]) -> None:
        MAX_BATCH_WRITE = 500

        col   = self.root.document(source_name).collection("courses")
        self.log.info(f"Saving {len(data)} course records to {source_name} in chunks of {MAX_BATCH_WRITE}")
        for i in range(0, len(data), MAX_BATCH_WRITE):
            batch = self.db.batch()
            chunk = data[i:i + MAX_BATCH_WRITE]
            for course in chunk:
                raw_code = course.get("course_code") # Attempt to pull course_code for ID
                if isinstance(raw_code, str) and raw_code.strip():
                    course_id = raw_code.strip()
                else:
                    raw_title = course.get("course_title", "") # Fall back to title if no valid course_code
                    course_id = self._slugify(raw_title)

                course_id = self._slugify(course_id)
                ref = col.document(course_id)
                batch.set(ref, course)  # overwrite if it already exists

            self.log.info(f"Saving {len(batch)} course records to {source_name}")
            await batch.commit()

class LocalFileStorage(StorageBackend):
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _ensure_dir(self, source_name: str) -> Path:
        out = self.base_dir / source_name
        out.mkdir(exist_ok=True)
        return out
    
    async def get_urls(self, source_name: str) -> List[str]:
        d = self.base_dir / source_name
        if not d.exists():
            return []
        try:
            with open(d / "urls.json", "r") as f:
                return json.load(f)
        except:
            return []

    async def save_urls(self, source_name: str, urls: List[str]) -> None:
        d = self._ensure_dir(source_name)
        with open(d / "urls.json", "w") as f:
            json.dump(list(urls), f, indent=2)

    async def save_schema(self, source_name: str, schema: Dict[str, Any]) -> None:
        d = self._ensure_dir(source_name)
        with open(d / "schema.json", "w") as f:
            json.dump(schema, f, indent=2)

    async def get_schema(self, source_name: str) -> Dict[str, Any]:
        d = self.base_dir / source_name
        if not d.exists():
            return {}
        try:
            with open(d / "schema.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    async def save_data(self, source_name: str, data: List[Dict[str, Any]]) -> None:
        d = self._ensure_dir(source_name)
        with open(d / "courses.json", "w") as f:
            json.dump(list(data), f, indent=2)

    async def get_data(self, source_name: str) -> List[Dict[str, Any]]:
        d = self.base_dir / source_name
        if not d.exists():
            return []
        try:
            with open(d / "courses.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return []