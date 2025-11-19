# remotive_ingest.py
"""
Fetch jobs from Remotive and store them in the Jobs Google Sheet.

Source: Remotive public API (remote-only jobs)
  https://remotive.com/api/remote-jobs

We map into the same Jobs sheet schema as RemoteOK:
    id, title, company, source, url, apply_url, source_job_id, location,
    job_roles, job_category, seniority, employment_type,
    tags, tech_stack, min_salary, max_salary, currency,
    high_salary, posted_at, ingested_at, remote_scope

We dedupe on (source, source_job_id).
"""

import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import requests
from dotenv import load_dotenv

from sheets_client import get_jobs_sheet
from remoteok_ingest import (
    _ensure_headers,
    _normalize_text,
    _to_str,
    _parse_float,
    extract_tech_stack,
    normalize_role,
    normalize_category,
    extract_seniority,
    extract_employment_type,
    is_high_salary,
    HIGH_SALARY_THRESHOLD,
    compute_remote_scope,
    clean_tags_list,
)

load_dotenv()

logger = logging.getLogger("remotive_ingest")
logger.setLevel(logging.INFO)

REMOTIVE_API_URL = os.environ.get("REMOTIVE_API_URL", "https://remotive.com/api/remote-jobs")


def _is_remote_or_hybrid(
    location: str,
    job_type: str | None = None,
    title: str | None = None,
    description: str | None = None,
) -> bool:
    """
    Accept:
    - Fully remote (worldwide / anywhere / remote)
    - Remote but region/country-limited (e.g. "USA", "EMEA", "LATAM")
    - Hybrid roles (where "hybrid" or "partly remote" appears)

    Reject:
    - Explicitly on-site only roles ("onsite only", "in-office only", "no remote", etc.)
    """
    loc = (location or "").lower()
    jt = (job_type or "").lower()
    text = " ".join([
        loc,
        jt,
        (title or "").lower(),
        (description or "").lower(),
    ])

    # 1) Hard reject explicit on-site only signals
    onsite_markers = [
        "onsite only",
        "on-site only",
        "in office only",
        "in-office only",
        "no remote",
        "not remote",
        "must be on site",
        "must be onsite",
    ]
    if any(p in text for p in onsite_markers):
        return False

    # 2) Accept if clearly remote
    remote_markers = [
        "remote",
        "work from home",
        "work-from-home",
        "anywhere",
        "worldwide",
        "global",
        "distributed team",
    ]
    if any(p in text for p in remote_markers):
        return True

    # 3) Accept hybrid roles explicitly
    hybrid_markers = [
        "hybrid",
        "partly remote",
        "part-remote",
    ]
    if any(p in text for p in hybrid_markers):
        return True

    # 4) Accept region/country-locked remote (Remotive context is remote-first)
    region_markers = [
        "usa",
        "united states",
        "canada",
        "uk",
        "united kingdom",
        "europe",
        "eu",
        "emea",
        "latam",
        "apac",
        "asia",
        "australia",
        "new zealand",
        "south africa",
        "brazil",
        "mexico",
        "argentina",
        "colombia",
        "india",
        "philippines",
    ]
    if any(r in loc for r in region_markers):
        return True

    # If it's something weird like a pure city name with no remote/region context,
    # be safe and drop it.
    return False


def _normalize_remotive_job(job: Dict[str, Any], headers: List[str]) -> Dict[str, Any] | None:
    """
    Map a Remotive job JSON object to our Jobs sheet columns.
    """
    job_id = job.get("id")
    if not job_id:
        return None

    source_job_id = _to_str(job_id)
    row_id = f"remotive_{source_job_id}"

    title = job.get("title") or ""
    company = job.get("company_name") or ""
    url = job.get("url") or ""

    location = job.get("candidate_required_location") or "Remote"
    job_type = job.get("job_type") or ""  # full_time / contract / part_time / freelance / internship
    description = job.get("description") or ""

    # Filter to remote / hybrid only (extra safety)
    if not _is_remote_or_hybrid(location, job_type, title, description):
        return None

    # Tags – Remotive gives a list of strings in "tags"
    raw_tags = job.get("tags") or []
    tags_list = clean_tags_list(raw_tags)
    tags_str = ", ".join(tags_list)

    tech_stack_list = extract_tech_stack(tags_list)
    tech_stack_str = ", ".join(tech_stack_list)

    # Salary – Remotive gives a free-text salary, often in USD like "$40,000 - $50,000"
    salary_text = (job.get("salary") or "").strip()
    min_salary_num: Optional[float] = None
    max_salary_num: Optional[float] = None
    currency = "USD"

    if salary_text:
        # Very light parsing: look for first two numbers
        import re

        nums = re.findall(r"[\d,]+", salary_text)
        if nums:
            min_salary_num = _parse_float(nums[0])
        if len(nums) >= 2:
            max_salary_num = _parse_float(nums[1])

    min_salary = "" if min_salary_num is None else min_salary_num
    max_salary = "" if max_salary_num is None else max_salary_num

    high_salary_flag = is_high_salary(min_salary_num, max_salary_num, currency, threshold=HIGH_SALARY_THRESHOLD)

    posted_at = _to_str(job.get("publication_date") or "")
    ingested_at = datetime.now(timezone.utc).isoformat()

    role = normalize_role(title, tags_list)
    category = normalize_category(title, tags_list, role)
    seniority = extract_seniority(title, tags_list)
    employment_type = extract_employment_type(job_type, tags_list)

    remote_scope = compute_remote_scope(location)

    row_dict: Dict[str, Any] = {
        "id": row_id,
        "title": title,
        "company": company,
        "source": "Remotive",
        "url": url,
        # Remotive's URL is both listing + apply, so we keep it in both fields
        "apply_url": url,
        "source_job_id": source_job_id,
        "location": location,
        "job_roles": role,
        "job_category": category,
        "seniority": seniority,
        "employment_type": employment_type,
        "tags": tags_str,
        "tech_stack": tech_stack_str,
        "min_salary": min_salary,
        "max_salary": max_salary,
        "currency": currency,
        "high_salary": "TRUE" if high_salary_flag else "FALSE",
        "posted_at": posted_at,
        "ingested_at": ingested_at,
        "remote_scope": remote_scope,
    }

    # Only keep keys present in sheet header
    for key in list(row_dict.keys()):
        if key not in headers:
            row_dict.pop(key, None)

    return row_dict


def ingest_remotive() -> int:
    """
    Fetch Remotive jobs and append new ones to the Jobs sheet.
    Returns the number of rows inserted.
    """
    sheet = get_jobs_sheet()
    headers = _ensure_headers(sheet)

    # Build existing key set (source:source_job_id)
    existing_records = sheet.get_all_records()
    existing_keys: Set[str] = set()
    for row in existing_records:
        source = _normalize_text(row.get("source"))
        sid = _normalize_text(str(row.get("source_job_id", "")))
        if source and sid:
            existing_keys.add(f"{source}:{sid}")

    logger.info("Loaded %d existing rows from Jobs sheet (for Remotive ingest)", len(existing_records))

    # Call Remotive API
    logger.info("Fetching jobs from Remotive API: %s", REMOTIVE_API_URL)
    resp = requests.get(
        REMOTIVE_API_URL,
        headers={"User-Agent": "ASAPJobsBot/1.0 (contact: youremail@example.com)"},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()

    jobs = data.get("jobs") or []
    logger.info("Fetched %d jobs from Remotive API", len(jobs))

    new_rows: List[List[Any]] = []
    inserted = 0

    for job in jobs:
        source_job_id = _to_str(job.get("id"))
        key = f"Remotive:{source_job_id}"

        if key in existing_keys:
            continue

        row_dict = _normalize_remotive_job(job, headers)
        if not row_dict:
            continue

        row_values = [row_dict.get(col, "") for col in headers]
        new_rows.append(row_values)
        inserted += 1

    if new_rows:
        logger.info("Appending %d new Remotive rows to Jobs sheet", len(new_rows))
        sheet.append_rows(new_rows, value_input_option="RAW")
    else:
        logger.info("No new Remotive jobs to insert")

    return inserted


if __name__ == "__main__":
    count = ingest_remotive()
    print(f"Ingested {count} new Remotive jobs.")
