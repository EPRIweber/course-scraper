# dashboard/backend/main.py

from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from .database import DashboardStorage
import traceback

app = FastAPI(title="Scraper Dashboard API")
storage = DashboardStorage()

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/api/school/{cleaned_name}/courses")
def get_school_courses(cleaned_name: str, limit: int = Query(5, ge=1, le=50)):
    try:
        data = storage.fetch_course_preview(cleaned_name, limit=limit)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/views")
def list_views():
    try:
        views = storage.list_views()
        return {"views": views}
    except Exception as e:
        print("ERROR in list_views:", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/view/{view_name}")
def get_view(view_name: str, limit: int = Query(100, ge=1, le=1000)):
    try:
        rows = storage.fetch_view(view_name, limit=limit)
        return {"data": rows}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print("ERROR in get_view:", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
