# dashboard/backend/database.py

import os
import asyncio
import pyodbc
from typing import Any, Dict, List, Optional

# Simple async wrapper around pyodbc for fetching scraper performance metrics.


def _build_conn_str() -> str:
    """Build connection string from environment variables."""
    dsn = os.getenv("DB_CONNECTION_STRING")
    if dsn:
        return dsn
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    if not all([server, database, user, password]):
        raise RuntimeError("Database credentials are not fully specified")
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};DATABASE={database};UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
    )


async def fetch_performance(
    limit: int = 100,
    offset: int = 0,
    source_name: Optional[str] = None,
    run_id: Optional[int] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch rows from scraper_performance view with optional filters."""
    conn_str = _build_conn_str()

    def _query() -> List[Dict[str, Any]]:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            sql = (
                "SELECT source_name, run_id, extracted_count, url_count, slots_left,"
                " DATEDIFF(second, start_scrape_ts, courses_extracted_ts) AS scrape_seconds,"
                " urls_per_second, records_per_second, inferred_max_concurrency,"
                " courses_extracted_ts, start_scrape_ts"
                " FROM scraper_performance WHERE 1=1"
            )
            params: List[Any] = []
            if source_name:
                sql += " AND source_name = ?"
                params.append(source_name)
            if run_id:
                sql += " AND run_id = ?"
                params.append(run_id)
            if start_ts:
                sql += " AND courses_extracted_ts >= ?"
                params.append(start_ts)
            if end_ts:
                sql += " AND courses_extracted_ts <= ?"
                params.append(end_ts)
            sql += " ORDER BY courses_extracted_ts DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
            params.extend([offset, limit])
            cursor.execute(sql, params)
            columns = [c[0] for c in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    return await asyncio.to_thread(_query)


async def fetch_schools_status() -> List[Dict[str, Any]]:
    """Fetch rows from current_progress_summary view."""
    conn_str = _build_conn_str()

    def _query() -> List[Dict[str, Any]]:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            sql = (
                "SELECT school_name, schema_count, url_count, course_count, has_courses,"
                " last_scrape_ts, summary_status, status_indicator"
                " FROM current_progress_summary"
            )
            cursor.execute(sql)
            columns = [c[0] for c in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    return await asyncio.to_thread(_query)


async def fetch_school_courses(
    cleaned_name: str, limit: int = 5
) -> Dict[str, Any]:
    """Return deduped course preview and count for a school."""
    conn_str = _build_conn_str()

    def _query() -> Dict[str, Any]:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            sample_sql = (
                "WITH sources_for_school AS ("
                "    SELECT source_id FROM stg_sources WHERE cleaned_name = ?"
                "), deduped_courses AS ("
                "    SELECT DISTINCT c.course_code, c.course_title, c.course_description,"
                "        c.course_credits, c.courses_crtd_dt"
                "    FROM stg_courses c JOIN sources_for_school s"
                "      ON c.course_source_id = s.source_id"
                ")"
                " SELECT TOP (?) course_code, course_title,"
                "     LEFT(course_description, 200) AS course_description_preview,"
                "     course_credits, courses_crtd_dt"
                " FROM deduped_courses ORDER BY courses_crtd_dt DESC"
            )
            count_sql = (
                "WITH sources_for_school AS ("
                "    SELECT source_id FROM stg_sources WHERE cleaned_name = ?"
                "), deduped_courses AS ("
                "    SELECT DISTINCT c.course_code, c.course_title"
                "    FROM stg_courses c JOIN sources_for_school s"
                "      ON c.course_source_id = s.source_id"
                ") SELECT COUNT(*) AS distinct_course_count FROM deduped_courses"
            )
            cursor.execute(sample_sql, (cleaned_name, limit))
            columns = [c[0] for c in cursor.description]
            courses = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.execute(count_sql, (cleaned_name,))
            count_row = cursor.fetchone()
            distinct_count = count_row[0] if count_row else 0
            return {
                "school_name": cleaned_name,
                "distinct_course_count": distinct_count,
                "sample_courses": courses,
            }

    return await asyncio.to_thread(_query)
