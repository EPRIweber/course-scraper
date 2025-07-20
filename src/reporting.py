# src/reporting.py
"""Helpers for printing job summaries to the log."""

import logging
from src.models import JobSummary

logger = logging.getLogger(__name__)

def generate_summary_report(summary: JobSummary):
    """
    Prints a formatted, human-readable summary of the entire scraping job
    to the console.
    """
    logger.info("=" * 60)
    logger.info("SCRAPING JOB SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Job ID: {summary.job_id}")

    if summary.end_time:
        duration = summary.end_time - summary.start_time
        logger.info(f"Run Time: {summary.start_time.isoformat()} to {summary.end_time.isoformat()}")
        logger.info(f"Total Duration: {duration}")
    else:
        logger.info(f"Start Time: {summary.start_time.isoformat()}")
        
    logger.info(f"Sources Attempted: {summary.total_sources}")
    logger.info(f"Succeeded: {summary.succeeded}")
    logger.info(f"Failed: {summary.failed}")
    logger.info("-" * 60)

    for result in sorted(summary.results, key=lambda r: r.source_name):
        logger.info(f"Source: {result.source_name} - Status: {result.status.upper()}")
        stats = result.stats
        logger.info(
            f"  Stats: URLs Found: {stats.urls_found}, Valid URLs: {stats.urls_valid}, "
            f"Extracted: {stats.records_extracted}, Validated: {stats.records_validated}"
        )
        if stats.records_missing_required_fields > 0:
            logger.warning(
                f"  Silent Error: {stats.records_missing_required_fields} records were discarded "
                f"due to missing required fields (e.g., title, description)."
            )

        if result.errors:
            logger.error(f"  Errors for {result.source_name}:")
            for error in result.errors:
                logger.error(f"    - Stage: '{error.stage}' at {error.timestamp.isoformat()}")
                logger.error(f"      Error: {error.exception_type} - {error.message}")
                # Stack traces are logged to the file but omitted from the console summary for brevity.
                # Use the log file for deep debugging.
                logger.debug(f"      Stack Trace:\n{error.stack_trace}")
    logger.info("=" * 60)
