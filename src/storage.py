# src/storage.py
"""Storage backends for scraped data.

Currently a SQL Server implementation is provided which exposes async methods
to save and fetch crawl results, schemas and classification data.
"""

import asyncio
import json
import pyodbc
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Sequence

from src.config import SourceConfig, config
from src.models import JobSummary

class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    @abstractmethod
    async def get_json_data(self, source_id: str) -> None: ...
    @abstractmethod
    async def log(self, run_id: int, src_id: str, stage: int, msg: str) -> None: ...
    @abstractmethod
    async def begin_run(self) -> int: ...
    @abstractmethod
    async def end_run(self, run_id: int) -> None: ...

    @abstractmethod
    async def list_sources(self) -> list[SourceConfig]: ...
    @abstractmethod
    async def ensure_source(self, src_cfg: SourceConfig) -> str: ...

    @abstractmethod
    async def get_urls(self, source_id: str) -> List[str]: ...
    @abstractmethod
    async def save_urls(self, source_id: str, urls: List[str]) -> None: ...
    @abstractmethod
    async def update_url_targets(self, source_id: str, good_urls: Sequence[str], bad_urls: Sequence[str]) -> None: ...

    @abstractmethod
    async def get_schema(self, source_id: str) -> Dict[str, Any]: ...
    @abstractmethod
    async def save_schema(self, source_id: str, schema: Dict[str, Any]) -> None: ...

    @abstractmethod
    async def get_data(self, source_id: str) -> List[Dict[str, Any]]: ...
    @abstractmethod
    async def save_data(self, source_id: str, data: List[Dict[str, Any]]) -> None: ...

    @abstractmethod
    async def get_classified(self, source_id: str) -> list[tuple[str, str]]: ...
    @abstractmethod
    async def save_classified(self, classified: list[tuple[str, list[str]]]) -> None: ...

class SqlServerStorage(StorageBackend):
    # ------------------------------------------------------------------ init
    def __init__(self, connect_str: str): # , loop: asyncio.AbstractEventLoop | None = None):
        self._conn_str = connect_str
        self._conn = pyodbc.connect(connect_str, autocommit=False)
        self._lock = asyncio.Lock()
        self._loop = None # loop or asyncio.get_event_loop()

    # ---------------------------------------------------------------- helpers
    def _run_sync(self, fn):
        loop = self._loop or asyncio.get_running_loop()
        self._loop = loop
        return loop.run_in_executor(None, fn)

    # async DB helpers
    async def _exec(self, sql: str, *p):
        async with self._lock:
            for attempt in range(2):
                try:
                    await self._run_sync(lambda: self._conn.execute(sql, *p).commit())
                    return
                except pyodbc.Error as e:
                    if e.args[0] in ('08S01', '08003', 'HYT00') and attempt == 0:
                        # Communication link failure / timeout → reconnect once
                        self._conn.close()
                        self._conn = pyodbc.connect(self._conn_str, autocommit=False)
                        continue
                    raise


    async def _fetch(self, sql: str, *p):
        async with self._lock:
            for attempt in range(2):
                try:
                    return await self._run_sync(lambda: self._conn.execute(sql, *p).fetchall())
                except pyodbc.Error as e:
                    if e.args[0] in ('08S01', '08003', 'HYT00') and attempt == 0:
                        self._conn.close()
                        self._conn = pyodbc.connect(self._conn_str, autocommit=False)
                        continue
                    raise


    # ------------------------------------------------------------------- runs
    async def begin_run(self) -> int:
        row = await self._fetch("EXEC dbo.begin_run")
        run_id = row[0][0] if row else None
        if run_id is None:
            raise RuntimeError("Another scrape is already running – mutex locked.")
        return run_id

    async def end_run(self, run_id: int):
        await self._exec(f"EXEC dbo.end_run ?", run_id)



    # ------------------------------------------------------------- source meta
    async def ensure_source(self, src_cfg: SourceConfig) -> str:
        """
        Insert (or fetch) the GUID of this source in `sources` and return it.
        """
        row = await self._fetch(
            "{CALL dbo.upsert_source(?,?,?,?,?,?,?,?,?,?)}",
            src_cfg.name,
            src_cfg.type,
            str(src_cfg.root_url),
            str(src_cfg.schema_url),
            src_cfg.crawl_depth,
            src_cfg.include_external,
            src_cfg.page_timeout_s,
            src_cfg.max_concurrency,
            src_cfg.url_base_exclude,
            json.dumps(src_cfg.url_exclude_patterns or [])
        )
        if not row or not hasattr(row[0], "source_id"):
            raise RuntimeError("Failed to fetch or insert source; no source_id returned.")
        return row[0].source_id

    async def list_sources(self) -> list[SourceConfig]:
        """Pulls sources from local yaml file, upserts to DB, returns all enabled sources from database."""
        local_sources = config.sources

        await asyncio.gather(*(self.ensure_source(src) for src in local_sources))

        rows = await self._fetch("EXEC dbo.get_enabled_sources")
        if not rows:
            return []
            
        return [
            SourceConfig(
                source_id=r.source_id,
                name=r.name,
                type=r.type,
                root_url=r.root_url,
                schema_url=r.schema_url,
                include_external=r.include_external,
                crawl_depth=r.crawl_depth,
                page_timeout_s=r.page_timeout_s,
                max_concurrency=r.max_concurrency,
                url_base_exclude=r.url_base_exclude,
                url_exclude_patterns=json.loads(r.url_exclude_patterns or "[]")
            )
            for r in rows
        ]


    async def log(self, run_id: int, src_id: str, stage: int, msg: str):
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
        if not rows:
            return []
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
        async with self._lock:
            def _bulk_insert():
                cur = self._conn.cursor()
                try:
                    for u in urls:
                        cur.execute(sql, source_id, u)
                    self._conn.commit()
                except Exception:
                    self._conn.rollback()
                    raise
            await self._run_sync(_bulk_insert)
    
    async def update_url_targets(self, source_id: str, good_urls: Sequence[str], bad_urls: Sequence[str]) -> None:
        """Update url is_target value based on if page contained target data"""
        if not good_urls and not bad_urls:
            return
        async with self._lock:
            def _run():
                cur = self._conn.cursor()
                
                try:
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
                except Exception:
                    self._conn.rollback()
                    raise

            await self._run_sync(_run)


    # -------------------------------------------------------------- schema JSON
    async def get_schema(self, source_id: str) -> Dict[str, Any]:
        """Fetch the scraper schema JSON for a source by calling a stored procedure."""
        # Updated to call the newly named stored procedure: dbo.get_schema
        rows = await self._fetch("{CALL dbo.get_schema(?)}", source_id)
        
        # The logic to handle the result remains the same.
        return json.loads(rows[0].scraper_schema_json) if rows else {}

    async def save_schema(self, source_id: str, schema: Dict[str, Any]) -> None:
        """Insert or update the scraper schema JSON for a source by calling a stored procedure."""
        # Serialize the dictionary to a JSON string before sending.
        schema_json = json.dumps(schema)

        # Call the dbo.save_schema stored procedure with the required parameters.
        await self._exec(
            "{CALL dbo.save_schema(?, ?)}",
            source_id,
            schema_json
        )

    # -------------------------------------------------------------- course records
    async def get_data(self, source_id: str) -> List[Dict[str, Any]]:
        """Fetch course records for a given source by calling a stored procedure."""
        # Call the dbo.get_data stored procedure.
        rows = await self._fetch("{CALL dbo.get_data(?)}", source_id)

        # The logic for processing the results remains unchanged.
        if not rows:
            return []
        return [
            {
                "course_id": r.course_id,
                "course_code": r.course_code,
                "course_title": r.course_title,
                "course_description": r.course_description,
                "course_credits": r.course_credits
            }
            for r in rows
        ]
    
    async def get_json_data(self, source_id: str) -> None:
        """TESTING FUNCTION, NOT MEANT FOR PRODUCTION USE"""
        rows = await self._fetch("{CALL dbo.get_data(?)}", source_id)
        courses = [
            {
                "course_code":       row.course_code,
                "course_title":      row.course_title,
                "course_description":row.course_description,
                "course_credits":    row.course_credits,
            }
            for row in rows
        ]
        with open("existing_courses.json", "w", encoding="utf-8") as f:
            json.dump(courses, f, indent=2, ensure_ascii=False)
    
    async def save_data(self, source_id: str, data: List[Dict[str, Any]]) -> None:
        if not data:
            return

        tvp_rows = [
            (
                rec.get("course_code") or None,
                rec.get("course_title") or None,
                rec.get("course_description") or None,
                rec.get("course_credits") or None
            )
            for rec in data
        ]

        sql = """
            DECLARE @t dbo.CourseData_v2;
            INSERT @t (course_code, course_title, course_description, course_credits)
            VALUES (?, ?, ?, ?);
            EXEC dbo.save_course_data ?, @t;
        """
        
        async with self._lock:
            def _bulk():
                cur = self._conn.cursor()
                try:
                    cur.fast_executemany = True
                    cur.executemany(sql, [(*row, source_id) for row in tvp_rows])
                    self._conn.commit()
                except Exception:
                    self._conn.rollback()
                    raise

            await self._run_sync(_bulk)

    async def get_classified(self, source_id: str) -> list[tuple[str, str]]:
        """
        Fetch all (course_id, taxonomy_id) pairs for the given source_id
        by joining course_taxonomy01 to courses.
        """
        # Query for all taxonomy assignments where the course belongs to this source
        sql = """
            SELECT ct.course_id, ct.taxonomy_id
            FROM dbo.course_taxonomy01 AS ct
            INNER JOIN dbo.courses AS c
              ON ct.course_id = c.course_id
            WHERE c.course_source_id = ?
        """
        rows = await self._fetch(sql, source_id)
        if not rows:
            return []
        # Each row has .course_id and .taxonomy_id attributes
        return [(row.course_id, row.taxonomy_id) for row in rows]

    async def save_classified(self, classified: list[tuple[str, list[str]]]) -> None:
        """
        Persist course → taxonomy relationships via a table-valued parameter.
        `classified` is a list of (course_id, [taxonomy_id, …]) tuples.
        """
        if not classified:
            return

        # Flatten into rows: one (course_id, taxonomy_id) per label
        tvp_rows = [
            (course_id, taxonomy_id)
            for course_id, taxonomy_ids in classified
            for taxonomy_id in taxonomy_ids
        ]

        sql = """
            DECLARE @t dbo.CourseTaxonomyData_v1;
            INSERT INTO @t (course_id, taxonomy_id) VALUES (?, ?);
            EXEC dbo.save_course_taxonomy @taxonomy_data = @t;
        """

        async with self._lock:
            def _bulk():
                cur = self._conn.cursor()
                try:
                    cur.fast_executemany = True
                    cur.executemany(sql, tvp_rows)
                    self._conn.commit()
                except Exception:
                    self._conn.rollback()
                    raise

            await self._run_sync(_bulk)
