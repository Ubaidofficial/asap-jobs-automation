# remotive_ingest.py
"""
Fetch jobs from Remotive and store them in the Jobs Google Sheet.

- Uses Remotive public API: https://remotive.com/api/remote-jobs
- Maps fields into your Jobs sheet columns:
    id, title, company, source, url, apply_url, source_job_id, location,
    job_roles, job_category, seniority, employment_type,
    tags, tech_stack, min_salary, max_salary, currency,
    high_salary, posted_at, ingested_at, remote_scope
- Skips jobs that already exist (same source + source_job_id)
- Filters to clearly remote-only jobs:
    1) remote_scope ∈ {global, country, regional}
    2) location text contains remote keywords (remote, worldwide, anywhere, etc.)
"""

from __future__ import annotations

import os
import re
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv

from sheets_client import get_jobs_sheet
from remoteok_ingest import (
    _ensure_headers,
    _normalize_text,
    _to_str,
    extract_tech_stack,
    normalize_role,
    normalize_category,
    extract_seniority,
    extract_employment_type,
    is_high_salary,
    HIGH_SALARY_THRESHOLD,
    compute_remote_scope,
)

load_dotenv()

logger = logging.getLogger("remotive_ingest")
logger.setLevel(logging.INFO)

REMOTIVE_API_URL = os.environ.get(
    "REMOTIVE_API_URL",
    "https://remotive.com/api/remote-jobs?limit=2000",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _looks_remote_location(location: str) -> bool:
    """
    Extra strict check to ensure the job is clearly remote, based on the raw
    location text Remotive provides.

    Examples that should pass:
      - "Remote"
      - "Remote - USA"
      - "US / Remote"
      - "Remote: Europe"
      - "Worldwide"
      - "Work from anywhere"
      - "Anywhere in the world"
    """
    loc = (location or "").strip()
    if not loc:
        return False

    lower = loc.lower()

    # Strong remote keywords
    remote_keywords = [
        "remote",
        "worldwide",
        "world wide",
        "anywhere",
        "work from anywhere",
        "work-from-anywhere",
        "work from home",
        "work-from-home",
    ]

    return any(k in lower for k in remote_keywords)


def _parse_salary_range(s: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Parse a Remotive salary string like:
      "$60,000 - $80,000"
      "USD 80k-120k"
      "€70,000"
    into (min_salary, max_salary, currency).

    Very best-effort, safe to fail to (None, None, "USD").
    """
    if not s:
        return None, None, "USD"

    text = s.strip()
    lower = text.lower()

    # Currency guessing
    if "$" in text or "usd" in lower:
        currency = "USD"
    elif "€" in text or "eur" in lower:
        currency = "EUR"
    elif "£" in text or "gbp" in lower:
        currency = "GBP"
    else:
        currency = "USD"

    # Extract all numeric chunks
    nums = re.findall(r"[\d,]+(?:\.\d+)?", text)
    if not nums:
        return None, None, currency

    def _to_float(num_str: str) -> Optional[float]:
        try:
            return float(num_str.replace(",", ""))
        except Exception:
            return None

    values = [_to_float(n) for n in nums]
    values = [v for v in values if v is not None]

    if not values:
        return None, None, currency

    if len(values) == 1:
        return values[0], None, currency

    return values[0], values[-1], currency


def _normalize_remotive_job(job: Dict[str, Any], headers: List[str]) -> Optional[Dict[str, Any]]:
    """
    Convert one Remotive job object into a dict keyed by column name.

    Only returns a row if:
      - remote_scope ∈ {global, country, regional}
      - AND the raw location text clearly indicates remote
        (remote / worldwide / anywhere / work from anywhere / etc.).
    """
    # Identify job
    raw_id = job.get("id") or job.get("job_id")
    if not raw_id:
        return None

    source_job_id = _to_str(raw_id)
    row_id = f"remotive_{source_job_id}"

    title = job.get("title") or ""
    company = job.get("company_name") or job.get("company") or ""

    # Remotive "url" usually points to the job page
    url = job.get("url") or ""

    # Location: Remotive uses "candidate_required_location" for geo
    location = (
        job.get("candidate_required_location")
        or job.get("location")
        or "Remote"
    )

    # Compute remote scope
    remote_scope = compute_remote_scope(location)

    # Filter to remote/hybrid only, and location text must clearly look remote
    if remote_scope not in {"global", "country", "regional"}:
        return None

    if not _looks_remote_location(location):
        return None

    # Tags & tech stack
    tags_list = job.get("tags") or job.get("job_tags") or []
    tags_str_list = [str(t).strip() for t in tags_list if str(t).strip()]
    tags_str = ", ".join(tags_str_list)
    tech_stack_list = extract_tech_stack(tags_str_list)
    tech_stack_str = ", ".join(tech_stack_list)

    # Salary parsing (best-effort)
    salary_str = job.get("salary") or ""
    min_salary_num, max_salary_num, currency = _parse_salary_range(salary_str)

    min_salary = "" if min_salary_num is None else min_salary_num
    max_salary = "" if max_salary_num is None else max_salary_num

    high_salary_flag = is_high_salary(min_salary_num, max_salary_num, currency, threshold=HIGH_SALARY_THRESHOLD)

    posted_at = _to_str(job.get("publication_date") or job.get("date") or "")
    ingested_at = _now_iso()

    role = normalize_role(title, tags_str_list)
    category = normalize_category(title, tags_str_list, role)
    seniority = extract_seniority(title, tags_str_list)
    employment_type = extract_employment_type(title, tags_str_list)

    # For Remotive, we'll use the Remotive URL as apply_url
    apply_url = url

    row_dict: Dict[str, Any] = {
        "id": row_id,
        "title": title,
        "company": company,
        "source": "Remotive",
        "url": url,
        "apply_url": apply_url,
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

    # Strip keys not present in the sheet headers
    for key in list(row_dict.keys()):
        if key not in headers:
            row_dict.pop(key, None)

    return row_dict


def ingest_remotive() -> int:
    """
    Fetch Remotive jobs and append new ones to the Jobs sheet.
    Returns the number of rows inserted (int).

    Only clearly remote jobs are inserted:
      - remote_scope ∈ {global, country, regional}
      - AND location text looks remote ("Remote", "Worldwide", "Anywhere", etc.)
    """
    sheet = get_jobs_sheet()
    headers = _ensure_headers(sheet)

    # Dedupe: build existing key set (source:source_job_id) from raw values
    all_values = sheet.get_all_values()
    existing_keys: Set[str] = set()

    def _find_col(name: str) -> Optional[int]:
        try:
            return headers.index(name)
        except ValueError:
            return None

    idx_source = _find_col("source")
    idx_sid = _find_col("source_job_id")

    if all_values and idx_source is not None and idx_sid is not None:
        for row in all_values[1:]:  # skip header
            src = _normalize_text(row[idx_source]) if idx_source < len(row) else ""
            sid = _normalize_text(row[idx_sid]) if idx_sid < len(row) else ""
            if src and sid:
                existing_keys.add(f"{src}:{sid}")

    logger.info(
        "Loaded %d existing rows from Jobs sheet (Remotive dedupe)",
        len(all_values) - 1 if all_values else 0,
    )

    # Call Remotive API
    headers_req = {
        "User-Agent": "ASAPJobsBot/1.0 (contact: youremail@example.com)",
    }
    logger.info("Fetching jobs from Remotive API: %s", REMOTIVE_API_URL)
    resp = requests.get(REMOTIVE_API_URL, headers=headers_req, timeout=25)
    resp.raise_for_status()
    data = resp.json()

    # Remotive API usually returns {"jobs": [ ... ]}
    if isinstance(data, dict) and "jobs" in data:
        jobs = [j for j in data["jobs"] if isinstance(j, dict)]
    elif isinstance(data, list):
        jobs = [j for j in data if isinstance(j, dict)]
    else:
        jobs = []

    logger.info("Fetched %d jobs from Remotive API", len(jobs))

    new_rows: List[List[Any]] = []
    inserted = 0

    for job in jobs:
        raw_id = job.get("id") or job.get("job_id")
        if not raw_id:
            continue

        source_job_id = _to_str(raw_id)
        key = f"Remotive:{source_job_id}"
        if key in existing_keys:
            continue

        row_dict = _normalize_remotive_job(job, headers)
        if not row_dict:
            continue  # filtered out (not clearly remote / invalid)

        row_values = [row_dict.get(col, "") for col in headers]
        new_rows.append(row_values)
        inserted += 1
        existing_keys.add(key)

    if new_rows:
        logger.info("Appending %d new Remotive rows to Jobs sheet", len(new_rows))
        sheet.append_rows(new_rows, value_input_option="RAW")
    else:
        logger.info("No new Remotive jobs to insert")

    return inserted


if __name__ == "__main__":
    count = ingest_remotive()
    print(f"Ingested {count} new Remotive jobs.")
