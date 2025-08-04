# dashboard/backend/database.py

import os
import pyodbc
from typing import Any, Dict, List, Optional
from src.storage import build_conn_str, fetch_all_sync, fetch_one_sync

class DashboardStorage:
    """Minimal read-only interface for the dashboard, reusing connection logic."""

    def __init__(self, connect_str: Optional[str] = None):
        self._conn_str = build_conn_str(connect_str)

    def fetch_performance(
        self,
        limit: int = 100,
        offset: int = 0,
        source_name: Optional[str] = None,
        run_id: Optional[int] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
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
        return fetch_all_sync(self._conn_str, sql, params)

    def fetch_progress_summary(self) -> List[Dict[str, Any]]:
        sql = (
            "SELECT school_name, schema_count, url_count, course_count,"
            " last_scrape_ts, summary_status, run_id, run_status"
            " FROM current_progress_summary"
            " ORDER BY last_scrape_ts DESC"
        )
        return fetch_all_sync(self._conn_str, sql)

    def fetch_course_preview(self, cleaned_name: str, limit: int = 5) -> Dict[str, Any]:
        sample_sql = (
            "WITH sources_for_school AS (SELECT source_id FROM stg_sources WHERE cleaned_name = ?), "
            "deduped_courses AS ("
            " SELECT DISTINCT c.course_code, c.course_title, c.course_description, "
            "        c.course_credits, c.courses_crtd_dt"
            " FROM stg_courses c JOIN sources_for_school s ON c.course_source_id = s.source_id"
            ") "
            "SELECT TOP (?) course_code, course_title, "
            " LEFT(course_description, 200) AS course_description_preview, "
            " course_credits, courses_crtd_dt "
            "FROM deduped_courses ORDER BY courses_crtd_dt DESC"
        )
        count_sql = (
            "WITH sources_for_school AS (SELECT source_id FROM stg_sources WHERE cleaned_name = ?), "
            "deduped_courses AS ("
            " SELECT DISTINCT c.course_code, c.course_title"
            " FROM stg_courses c JOIN sources_for_school s ON c.course_source_id = s.source_id"
            ") "
            "SELECT COUNT(*) FROM deduped_courses"
        )
        courses = fetch_all_sync(self._conn_str, sample_sql, (cleaned_name, limit))
        distinct_count_row = fetch_one_sync(self._conn_str, count_sql, (cleaned_name,))
        distinct_count = distinct_count_row[0] if distinct_count_row else 0
        return {
            "school_name": cleaned_name,
            "distinct_course_count": distinct_count,
            "sample_courses": courses,
        }
