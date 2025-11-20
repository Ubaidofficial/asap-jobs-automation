# weworkremotely_ingest.py
"""
Fetch jobs from WeWorkRemotely and store them in the Jobs Google Sheet.

Because WWR doesn't have a public JSON API, we do simple HTML scraping.

We map into the same Jobs sheet schema:
    id, title, company, source, url, apply_url, source_job_id, location,
    job_roles, job_category, seniority, employment_type,
    tags, tech_stack, min_salary, max_salary, currency,
    high_salary, posted_at, ingested_at, remote_scope

We dedupe on (source, source_job_id) and only insert jobs where
remote_scope ∈ {global, country, regional}.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
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

logger = logging.getLogger("weworkremotely_ingest")
logger.setLevel(logging.INFO)

WWR_URL = os.environ.get("WWR_URL", "https://weworkremotely.com/remote-jobs")
USER_AGENT = "ASAPJobsBot/1.0 (contact: youremail@example.com)"


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_html(url: str) -> Optional[str]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=25,
        )
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", url, e)
        return None


def _normalize_wwr_job(job: Dict[str, Any], headers: List[str]) -> Optional[Dict[str, Any]]:
    """
    job keys: title, company, url, location
    """
    title = job.get("title") or ""
    company = job.get("company") or ""
    url = job.get("url") or ""
    location = job.get("location") or "Remote"

    # classify remote scope; only insert remote/hybrid
    remote_scope = compute_remote_scope(location)
    if remote_scope not in {"global", "country", "regional"}:
        return None

    # source_job_id: last path segment of URL
    path_part = url.split("?")[0].rstrip("/").split("/")[-1] if url else title
    source_job_id = _to_str(path_part)
    row_id = f"wwr_{source_job_id}"

    tags_list: List[str] = []
    tags_str = ", ".join(tags_list)
    tech_stack_list = extract_tech_stack(tags_list)
    tech_stack_str = ", ".join(tech_stack_list)

    role = normalize_role(title, tags_list)
    category = normalize_category(title, tags_list, role)
    seniority = extract_seniority(title, tags_list)
    employment_type = extract_employment_type(title, tags_list)

    # Salary rarely present → leave blank
    min_salary_num: Optional[float] = None
    max_salary_num: Optional[float] = None
    currency = "USD"
    min_salary = ""
    max_salary = ""
    high_salary_flag = is_high_salary(min_salary_num, max_salary_num, currency, threshold=HIGH_SALARY_THRESHOLD)

    posted_at = ""  # WWR lists relative time; we could parse later if needed
    ingested_at = _now_iso()

    row_dict: Dict[str, Any] = {
        "id": row_id,
        "title": title,
        "company": company,
        "source": "WeWorkRemotely",
        "url": url,
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

    for key in list(row_dict.keys()):
        if key not in headers:
            row_dict.pop(key, None)

    return row_dict


def ingest_weworkremotely() -> int:
    """
    Fetch WeWorkRemotely jobs and append new ones to the Jobs sheet.
    Returns number of rows inserted.
    """
    sheet = get_jobs_sheet()
    headers = _ensure_headers(sheet)

    # Build existing key set (source:source_job_id)
    existing_records = sheet.get_all_records(expected_headers=headers)
    existing_keys: Set[str] = set()
    for row in existing_records:
        source = _normalize_text(row.get("source"))
        sid = _normalize_text(str(row.get("source_job_id", "")))
        if source and sid:
            existing_keys.add(f"{source}:{sid}")

    logger.info("Loaded %d existing rows from Jobs sheet (for WWR ingest)", len(existing_records))

    html = _fetch_html(WWR_URL)
    if not html:
        logger.warning("No HTML fetched from WWR_URL")
        return 0

    soup = BeautifulSoup(html, "html.parser")
    new_rows: List[List[Any]] = []
    inserted = 0

    # WWR structure may change; this is a best-effort parser.
    for section in soup.select("section.jobs"):
        for li in section.select("li"):
            # skip "view-all" / "load more" list items
            if "view-all" in " ".join(li.get("class", [])):
                continue

            a = li.find("a", href=True)
            if not a:
                continue

            href = a["href"]
            url = urljoin(WWR_URL, href)

            company_el = li.find("span", class_="company")
            title_el = li.find("span", class_="title")
            location_el = li.find("span", class_="region")

            company = (company_el.get_text() or "").strip() if company_el else ""
            title = (title_el.get_text() or "").strip() if title_el else (a.get_text() or "").strip()
            location = (location_el.get_text() or "").strip() if location_el else "Remote"

            raw_job = {
                "title": title,
                "company": company,
                "url": url,
                "location": location,
            }

            row_dict = _normalize_wwr_job(raw_job, headers)
            if not row_dict:
                continue

            key = f"WeWorkRemotely:{row_dict.get('source_job_id')}"
            if key in existing_keys:
                continue

            row_values = [row_dict.get(col, "") for col in headers]
            new_rows.append(row_values)
            existing_keys.add(key)
            inserted += 1

    if new_rows:
        logger.info("Appending %d new WWR rows to Jobs sheet", len(new_rows))
        sheet.append_rows(new_rows, value_input_option="RAW")
    else:
        logger.info("No new WeWorkRemotely jobs to insert")

    return inserted


if __name__ == "__main__":
    count = ingest_weworkremotely()
    print(f"Ingested {count} new WeWorkRemotely jobs.")
