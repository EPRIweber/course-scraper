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
        """Fetch first 100 course records for a given source by calling a stored procedure."""
        # Call the dbo.get_data stored procedure.
        rows = await self._fetch("{CALL dbo.get_data(?)}", source_id)

        # The logic for processing the results remains unchanged.
        return [
            {
                "course_code": r.course_code,
                "course_title": r.course_title,
                "course_description": r.course_description,
            }
            for r in rows
        ]
    
    async def save_data(self, source_id: str, data: List[Dict[str, Any]]) -> None:
        """Insert or update course records by sending them in bulk to a stored procedure."""
        if not data:
            return

        # Prepare the data as a list of tuples in the exact order
        # of the columns defined in our 'dbo.CourseData_v1' table type.
        params = [
            (
                rec.get("course_code"),
                rec.get("course_title"),
                rec.get("course_description"),
            )
            for rec in data
        ]

        # The SQL to execute now simply calls the stored procedure.
        # The second parameter is our list of tuples, which pyodbc will
        # pass as the table-valued parameter.
        sql = "{CALL dbo.save_course_data(?, ?)}"

        def _bulk_insert():
            # Using a cursor's 'fast_executemany' mode is highly recommended for performance
            # with table-valued parameters, though it might require specific driver settings.
            # self._conn.fast_executemany = True # Optional but recommended
            with self._conn.cursor() as cur:
                cur.execute(sql, source_id, params)
            self._conn.commit()

        await self._run_sync(_bulk_insert)

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