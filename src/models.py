# src/models.py
from pydantic import BaseModel, Field
from typing import List, Optional, Any
from datetime import datetime, timezone
import traceback

class ErrorLog(BaseModel):
    """
    A structured model for logging errors that occur during a scrape.
    """
    # Use a lambda to ensure the default is a new timezone-aware datetime for each instance.
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_name: str
    stage: str  # e.g., "crawl", "schema", "scrape", "storage"
    message: str
    exception_type: Optional[str] = None
    stack_trace: Optional[str] = None

    @classmethod
    def from_exception(cls, e: Exception, source_name: str, stage: str):
        """Creates an ErrorLog instance from an exception object."""
        return cls(
            source_name=source_name,
            stage=stage,
            message=str(e),
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
        )

class RunStatistics(BaseModel):
    """
    Holds statistics for a single source run.
    """
    urls_found: int = 0
    urls_valid: int = 0
    records_extracted: int = 0
    records_validated: int = 0
    records_missing_required_fields: int = 0

class SourceRunResult(BaseModel):
    """
    Represents the outcome of processing a single source.
    """
    source_name: str
    status: str  # "success", "failure", "in-progress"
    # Use a lambda to ensure the default is a new timezone-aware datetime for each instance.
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    stats: RunStatistics = Field(default_factory=RunStatistics)
    errors: List[ErrorLog] = []

class JobSummary(BaseModel):
    """
    Represents the summary of an entire scraping job, encompassing all sources.
    This is the object that gets saved to storage for monitoring.
    """
    job_id: str
    # Use a lambda to ensure the default is a new timezone-aware datetime for each instance.
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    total_sources: int
    succeeded: int = 0
    failed: int = 0
    results: List[SourceRunResult] = []
