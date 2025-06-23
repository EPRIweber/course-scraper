# src/storage.py
import asyncio
import json
import pyodbc
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Sequence

from src.config import SourceConfig
from src.models import JobSummary

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

class SqlServerStorage(StorageBackend):
    # ------------------------------------------------------------------ init
    def __init__(self, connect_str: str): # , loop: asyncio.AbstractEventLoop | None = None):
        self._conn = pyodbc.connect(connect_str, autocommit=False)
        self._loop = None # loop or asyncio.get_event_loop()

    # ---------------------------------------------------------------- helpers
    def _run_sync(self, fn):          # tiny wrapper to push sync work off-thread
        loop = self._loop or asyncio.get_running_loop()
        self._loop = loop
        return loop.run_in_executor(None, fn)

    # async DB helpers
    async def _exec(self, sql: str, *p):
        await self._run_sync(lambda: self._conn.execute(sql, *p).commit())

    async def _fetch(self, sql: str, *p):
        return await self._run_sync(lambda: self._conn.execute(sql, *p).fetchall())

    # ------------------------------------------------------------- source meta
    async def ensure_source(self, src_cfg: SourceConfig) -> str:
        """
        Insert (or fetch) the GUID of this source in `sources` and return it.
        """
        row = await self._fetch(
            "{CALL dbo.upsert_source(?,?,?,?,?,?)}",
            src_cfg.name,
            src_cfg.type,
            str(src_cfg.root_url),
            str(src_cfg.schema_url),
            getattr(src_cfg, "pdf_url", None),
            src_cfg.crawl_depth
        )
        return row[0].source_id

    # ------------------------------------------------------------------- runs
    async def new_run(self) -> str:
        """Insert a row into `runs`; return the GUID."""
        return await self._fetch("EXEC dbo.begin_run").fetchone().run_id

    async def log(self, run_id: str, src_id: str, stage: int, msg: str):
        """Insert a log message for a run and source."""
        await self._exec(
            "INSERT INTO logs (log_run_id,log_source_id,log_stage,log_message,log_ts)"
            "VALUES (?,?,?,?,SYSUTCDATETIME())",
            run_id, src_id, stage, msg
        )

    # -------------------------------------------------------------------- URLS
    async def get_urls(self, source_id: str) -> List[str]:
        """Fetch URLs for a source that are marked as targets."""
        rows = await self._fetch("{CALL dbo.get_target_urls(?)}", source_id)
        return [r.url_link for r in rows]

    async def save_urls(self, source_id: str, urls: Sequence[str]) -> None:
        """Insert or update URLs for a source."""
        sql = """
        MERGE urls WITH (HOLDLOCK) AS t
        USING (SELECT ? AS source_id, ? AS link) AS s
        ON t.url_source_id = s.source_id AND t.url_link = s.link
        WHEN NOT MATCHED THEN
          INSERT (url_source_id,url_link,is_target) VALUES (s.source_id,s.link,1);
        """
        def _bulk_insert():
            cur = self._conn.cursor()
            for u in urls:
                cur.execute(sql, source_id, u)
            self._conn.commit()
        await self._run_sync(_bulk_insert)

    # -------------------------------------------------------------- schema JSON
    async def get_schema(self, source_id: str) -> Dict[str, Any]:
        """Fetch the scraper schema JSON for a source."""
        rows = await self._fetch(
            """SELECT ss.scraper_schema_json
                 FROM scraper_schemas ss
                 JOIN sources s ON s.source_id = ss.scraper_schema_source_id
                WHERE s.source_id = ?""",
            source_id,
        )
        return json.loads(rows[0].scraper_schema_json) if rows else {}

    async def save_schema(self, source_id: str, schema: Dict[str, Any]) -> None:
        """Insert or update the scraper schema JSON for a source."""
        await self._exec(
            """
            MERGE scraper_schemas AS t
            USING (SELECT ? AS id, ? AS js) AS s
            ON t.scraper_schema_source_id = s.id
            WHEN MATCHED THEN UPDATE SET scraper_schema_json = s.js
            WHEN NOT MATCHED THEN INSERT (scraper_schema_source_id,scraper_schema_json)
                 VALUES(s.id,s.js);
            """,
            source_id, json.dumps(schema)
        )

    # -------------------------------------------------------------- course records
    async def get_data(self, source_id: str) -> List[Dict[str, Any]]:
        """Fetch first 100 course records for given source."""
        rows = await self._fetch(
            """SELECT TOP 100 c.course_code, c.course_title, c.course_description
                FROM courses c
                JOIN sources s ON s.source_id = c.course_source_id
                WHERE s.source_id = ?;""",
            source_id,
        )
        return [
            {
                "course_code": r.course_code,
                "course_title": r.course_title,
                "course_description": r.course_description,
            }
            for r in rows
        ]
    
    async def save_data(self, source_id: str, data: List[Dict[str, Any]]) -> None:
        """Insert or update course records in the database."""
        if not data: return
        sql = """
        MERGE courses WITH(HOLDLOCK) AS t
        USING (SELECT ? AS sid, ? AS code, ? AS title, ? AS description) AS s
        ON t.course_source_id = s.sid AND COALESCE(t.course_code,'') = COALESCE(s.code,'') 
           AND t.course_title = s.title
        WHEN MATCHED THEN UPDATE
             SET course_description = s.description
        WHEN NOT MATCHED THEN
             INSERT (course_source_id,course_code,course_title,course_description)
             VALUES (s.sid,s.code,s.title,s.description);
        """
        def _bulk():
            cur = self._conn.cursor()
            for rec in data:
                cur.execute(sql,
                    source_id,
                    rec.get("course_code"),
                    rec["course_title"],
                    rec["course_description"],
                )
            self._conn.commit()
        await self._run_sync(_bulk)

    async def update_url_targets(self, source_id: str, good_urls: Sequence[str], bad_urls: Sequence[str]) -> None:
        """Update url is_target value based on if page contained target data"""
        if not good_urls and not bad_urls:
            return

        def _run():
            cur = self._conn.cursor()

            if good_urls:
                # any url in *good_urls* is definitely still a target
                cur.executemany(
                    "UPDATE urls SET is_target = 1 "
                    "WHERE url_source_id = ? AND url_link = ?",
                    [(source_id, u) for u in good_urls]
                )
            if bad_urls:
                # pages crawled but produced nothing → toggle off
                cur.executemany(
                    "UPDATE urls SET is_target = 0 "
                    "WHERE url_source_id = ? AND url_link = ?",
                    [(source_id, u) for u in bad_urls]
                )
            self._conn.commit()

        await self._run_sync(_run)
    
    async def end_run(self, run_id: int):
        await self._exec(f"EXEC dbo.end_run(?)", run_id)