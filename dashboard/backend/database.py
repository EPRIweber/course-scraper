# dashboard/backend/database.py

import os
# import pyodbc
import re
from typing import Any, Dict, List, Optional
from src.storage import build_conn_str, fetch_all_sync, fetch_one_sync

class DashboardStorage:
    """Minimal read-only interface for the dashboard, reusing connection logic."""

    def __init__(self, connect_str: Optional[str] = None):
        self._conn_str = build_conn_str(connect_str)

    def fetch_course_preview(self, cleaned_name: str, limit: int = 5) -> Dict[str, Any]:
        sample_sql = (
            "WITH sources_for_school AS (SELECT source_id FROM sources WHERE cleaned_name = ?), "
            "deduped_courses AS ("
            " SELECT DISTINCT c.course_code, c.course_title, c.course_description, "
            "        c.course_credits, c.courses_crtd_dt"
            " FROM courses c JOIN sources_for_school s ON c.course_source_id = s.source_id"
            ") "
            "SELECT TOP (?) course_code, course_title, "
            " LEFT(course_description, 200) AS course_description_preview, "
            " course_credits, courses_crtd_dt "
            "FROM deduped_courses ORDER BY courses_crtd_dt DESC"
        )
        count_sql = (
            "SELECT course_count "
            "FROM source_data_status "
            "WHERE cleaned_name = ?"
        )
        courses = fetch_all_sync(self._conn_str, sample_sql, (cleaned_name, limit))
        distinct_count_row = fetch_one_sync(self._conn_str, count_sql, (cleaned_name,))
        distinct_count = distinct_count_row[0] if distinct_count_row else 0
        return {
            "school_name": cleaned_name,
            "distinct_course_count": distinct_count,
            "sample_courses": courses,
        }

    def list_views(self) -> List[str]:
        sql = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS "
            "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME LIKE 'dashboard\\_%' ESCAPE '\\' "
            "ORDER BY TABLE_NAME"
        )
        rows = fetch_all_sync(self._conn_str, sql)
        return [row["TABLE_NAME"] for row in rows]
    
    def _first_ts_column(self, view: str) -> Optional[str]:
        """Look up the first (by ordinal position) column in this view whose name ends in 'ts'."""
        sql = """
        SELECT COLUMN_NAME
          FROM INFORMATION_SCHEMA.COLUMNS
         WHERE TABLE_SCHEMA = 'dbo'
           AND TABLE_NAME = ?
           AND COLUMN_NAME LIKE '%ts'
         ORDER BY ORDINAL_POSITION
        """
        cols = fetch_all_sync(self._conn_str, sql, [view])
        return cols[0]["COLUMN_NAME"] if cols else None

    def fetch_view(self, view: str,  limit: int = 100) -> List[Dict[str, Any]]:
        """SELECT * from the view, dynamically ORDER BY its first '*ts' column if one exists."""
        ts_col = self._first_ts_column(view)
        if ts_col:
            q = f"SELECT TOP({limit}) * FROM dbo.{view} ORDER BY [{ts_col}] DESC"
        else:
            q = f"SELECT TOP({limit}) * FROM dbo.{view}"
        return fetch_all_sync(self._conn_str, q)
