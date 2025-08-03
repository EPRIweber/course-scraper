# dashboard/backend/main.py

from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from . import database


app = FastAPI(title="Scraper Performance API")


@app.get("/api/performance")
async def get_performance(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    source_name: Optional[str] = None,
    run_id: Optional[int] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
) -> List[dict]:
    """Return scraper performance metrics with optional filtering."""
    try:
        rows = await database.fetch_performance(
            limit=limit,
            offset=offset,
            source_name=source_name,
            run_id=run_id,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        return {"data": rows, "limit": limit, "offset": offset}
    except Exception as exc:  # pragma: no cover - simple MVP error handling
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/schools_status")
async def get_schools_status() -> List[dict]:
    """Return progress summary for all schools."""
    try:
        rows = await database.fetch_schools_status()
        return {"data": rows}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/school/{cleaned_name}/courses")
async def get_school_courses(
    cleaned_name: str, limit: int = Query(5, ge=1, le=50)
) -> dict:
    """Return a preview sample of courses for a school."""
    try:
        data = await database.fetch_school_courses(cleaned_name, limit=limit)
        return data
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc))
