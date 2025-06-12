# src/storage.py
import json
from pathlib import Path
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from google.cloud import firestore

class StorageBackend(ABC):
    @abstractmethod
    def get_urls(self, source: str) -> List[str]: ...
    @abstractmethod
    def save_urls(self, source: str, urls: List[str]) -> None: ...

    @abstractmethod
    def get_schema(self, source: str) -> Dict[str, Any]: ...
    @abstractmethod
    def save_schema(self, source: str, schema: Dict[str, Any]) -> None: ...

    @abstractmethod
    def get_data(self, source: str) -> List[Dict[str, Any]]: ...
    @abstractmethod
    def save_data(self, source: str, data: List[Dict[str, Any]]) -> None: ...

class FirestoreStorage(StorageBackend):
    def __init__(self, project=None):
        self.client = firestore.Client(project=project)
        self.root = self.client.collection("scraper")

    def _school_doc(self, source_name: str):
        return self.root.document(source_name)

    def get_urls(self, source_name: str) -> List[str]:
        sub = self._school_doc(source_name).collection("urls")
        doc = sub.document("list").get()
        return (doc.to_dict() or {}).get("items", [])

    def save_urls(self, source_name: str, urls: List[str]) -> None:
        sub = self._school_doc(source_name).collection("urls")
        sub.document("list").set({"items": urls})

    def get_schema(self, source_name: str) -> Dict[str, Any]:
        sub = self._school_doc(source_name).collection("schema")
        doc = sub.document("definition").get()
        return doc.to_dict() or {}

    def save_schema(self, source_name: str, schema: Dict[str, Any]) -> None:
        sub = self._school_doc(source_name).collection("schema")
        sub.document("definition").set(schema)

    def get_data(self, source_name: str) -> List[Dict[str, Any]]:
        sub = self._school_doc(source_name).collection("courses")
        doc = sub.document("all").get()
        return (doc.to_dict() or {}).get("records", [])

    def save_data(self, source_name: str, data: List[Dict[str, Any]]) -> None:
        courses = self._school_doc(source_name).collection("courses")
        for course in data:
            course_id = course.get("course_code", "").replace(" ", "_") + course.get("course_title", "").replace(" ", "_")
            courses.document(course_id).set(course)

class LocalFileStorage(StorageBackend):
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