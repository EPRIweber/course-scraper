# src/storage.py
import json
from pathlib import Path
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from google.cloud import firestore

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

    async def get_urls(self, source_name: str) -> List[str]:
        doc = self.root.document(source_name).collection("urls").document("list")
        snap = await doc.get()
        return (snap.to_dict() or {}).get("items", [])

    async def save_urls(self, source_name: str, urls: List[str]) -> None:
        doc = self.root.document(source_name).collection("urls").document("list")
        await doc.set({"items": urls})

    async def get_schema(self, source_name: str) -> Dict[str, Any]:
        doc = self.root.document(source_name).collection("schema").document("definition")
        snap = await doc.get()
        return snap.to_dict() or {}

    async def save_schema(self, source_name: str, schema: Dict[str, Any]) -> None:
        doc = self.root.document(source_name).collection("schema").document("definition")
        await doc.set(schema)

    async def get_data(self, source_name: str) -> List[Dict[str, Any]]:
        col = self.root.document(source_name).collection("courses")
        docs = [d async for d in col.stream()]
        return [d.to_dict() for d in docs]

    async def save_data(self, source_name: str, data: List[Dict[str, Any]]) -> None:
        batch = self.db.batch()
        col   = self.root.document(source_name).collection("courses")
        for course in data:
            cid = course.get("course_code", "").replace(" ", "_") or course.get("course_title","no_title")
            ref = col.document(cid)
            batch.set(ref, course)
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