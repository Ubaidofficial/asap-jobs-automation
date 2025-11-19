# remoteok_ingest.py
"""
Fetch jobs from RemoteOK and store them in the Jobs Google Sheet.

- Uses RemoteOK public API: https://remoteok.com/api  (24h delayed feed)
- Maps fields into your Jobs sheet columns:
    id, title, company, source, url, source_job_id, location,
    job_roles, job_category, seniority, employment_type,
    tags, tech_stack, min_salary, max_salary, currency,
    high_salary, posted_at, ingested_at
- Skips jobs that already exist (same source + source_job_id)
"""

import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

from sheets_client import get_jobs_sheet

load_dotenv()

REMOTEOK_API_URL = os.environ.get("REMOTEOK_API_URL", "https://remoteok.com/api")


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def _normalize_remoteok_job(job: Dict[str, Any]) -> List[str] | None:
    """
    Convert one RemoteOK job object into a row matching the Jobs sheet header.
    Returns a list of strings in the exact column order, or None to skip.
    """
    job_id = job.get("id")
    if not job_id:
        return None

    source_job_id = _to_str(job_id)
    row_id = f"remoteok_{source_job_id}"

    title = job.get("position") or job.get("title") or ""
    company = job.get("company") or ""
    url = job.get("url") or job.get("apply_url") or ""

    # Location: RemoteOK often has "location" or "region"
    location = job.get("location") or job.get("region") or job.get("country") or "Remote"

    # Tags & tech stack
    tags_list = job.get("tags") or []
    if isinstance(tags_list, list):
        tags = ", ".join(_to_str(t) for t in tags_list)
    else:
        tags = _to_str(tags_list)

    tech_stack = tags  # for now, just mirror tags

    # Salary info (best-effort; many jobs don’t have these)
    min_salary = _to_str(job.get("salary_min") or "")
    max_salary = _to_str(job.get("salary_max") or "")
    currency = _to_str(job.get("salary_currency") or "")

    # Posted date – RemoteOK uses "date" or "created_at"
    posted_at = _to_str(job.get("date") or job.get("created_at") or "")

    ingested_at = datetime.now(timezone.utc).isoformat()

    # Order must match your Jobs sheet header:
    # id, title, company, source, url, source_job_id, location,
    # job_roles, job_category, seniority, employment_type,
    # tags, tech_stack, min_salary, max_salary, currency,
    # high_salary, posted_at, ingested_at
    row = [
        row_id,            # id
        title,             # title
        company,           # company
        "RemoteOK",        # source
        url,               # url
        source_job_id,     # source_job_id
        location,          # location
        "",                # job_roles (we'll enrich later)
        "",                # job_category
        "",                # seniority
        "",                # employment_type
        tags,              # tags
        tech_stack,        # tech_stack
        min_salary,        # min_salary
        max_salary,        # max_salary
        currency,          # currency
        "",                # high_salary (can compute later)
        posted_at,         # posted_at
        ingested_at,       # ingested_at
    ]
    return row


def ingest_remoteok() -> int:
    """
    Fetch RemoteOK jobs and append new ones to the Jobs sheet.
    Returns the number of rows inserted.
    """
    sheet = get_jobs_sheet()

    # Existing jobs – to avoid duplicates (per source_job_id for RemoteOK)
    existing_records = sheet.get_all_records()
    existing_remoteok_ids = {
        str(row.get("source_job_id"))
        for row in existing_records
        if (row.get("source") or "").lower() == "remoteok"
    }

    # Call RemoteOK API
    headers = {
        "User-Agent": "ASAPJobsBot/1.0 (contact: youremail@example.com)"
    }
    resp = requests.get(REMOTEOK_API_URL, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    # RemoteOK returns a list; first element is metadata, then jobs
    if isinstance(data, list):
        jobs = [j for j in data if isinstance(j, dict) and j.get("id")]
    else:
        jobs = []

    new_rows: List[List[str]] = []
    for job in jobs:
        sid = str(job.get("id"))
        if sid in existing_remoteok_ids:
            continue  # already stored

        row = _normalize_remoteok_job(job)
        if row:
            new_rows.append(row)

    if new_rows:
        sheet.append_rows(new_rows, value_input_option="RAW")

    return len(new_rows)


if __name__ == "__main__":
    inserted = ingest_remoteok()
    print(f"Ingested {inserted} new RemoteOK jobs.")
