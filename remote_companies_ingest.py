# remote_companies_ingest.py
"""
Ingest jobs directly from remote-first companies' careers pages.

- Uses REMOTE_COMPANIES list from remote_companies_config.py
- Supports Greenhouse and Lever job boards (most remote-first companies use these)
- Writes into the same Jobs Google Sheet schema as RemoteOK/Remotive:
    id, title, company, source, url, apply_url, source_job_id, location,
    job_roles, job_category, seniority, employment_type,
    tags, tech_stack, min_salary, max_salary, currency,
    high_salary, posted_at, ingested_at, remote_scope

- Dedupe on (source, source_job_id)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from sheets_client import get_jobs_sheet
from remote_companies_config import REMOTE_COMPANIES
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

logger = logging.getLogger("remote_companies_ingest")
logger.setLevel(logging.INFO)

USER_AGENT = "ASAPJobsBot/1.0 (contact: youremail@example.com)"


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------


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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_source_job_id(company_slug: str, external_id: str) -> str:
    # external_id could be Greenhouse job id, Lever id, or a hash of title+location
    return f"{company_slug}:{external_id}"


# --------------------------------------------------------------------
# Greenhouse parser
# --------------------------------------------------------------------


def _parse_greenhouse_board(
    html: str,
    base_url: str,
    company_slug: str,
    company_name: str,
    default_location: str = "Remote",
) -> List[Dict[str, Any]]:
    """
    Parse a Greenhouse-hosted board (boards.greenhouse.io).

    Greenhouse markup is quite consistent:
    - Job links are within <a> tags under .opening or .opening a, or inside <div class="opening">
    """
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[Dict[str, Any]] = []

    # Classic pattern: <div class="opening"> <a href="/company/12345">Title</a> <span class="location">Remote</span>
    for div in soup.select("div.opening"):
        a = div.find("a", href=True)
        if not a:
            continue

        title = (a.get_text() or "").strip()
        href = a["href"]
        url = urljoin(base_url, href)

        # Often location is in span.location
        loc_el = div.find(class_="location")
        location = (loc_el.get_text() or "").strip() if loc_el else default_location

        # Try to extract an external id from href: the numeric part at the end
        external_id = href.rstrip("/").split("/")[-1]

        jobs.append(
            {
                "title": title,
                "company": company_name,
                "company_slug": company_slug,
                "external_id": external_id,
                "url": url,
                "location": location or default_location,
            }
        )

    return jobs


# --------------------------------------------------------------------
# Lever parser
# --------------------------------------------------------------------


def _parse_lever_board(
    html: str,
    base_url: str,
    company_slug: str,
    company_name: str,
    default_location: str = "Remote",
) -> List[Dict[str, Any]]:
    """
    Parse a Lever job board (jobs.lever.co).

    Typical Lever markup:
    - <div class="posting">
        <a class="posting-title" href="/company/1234">...</a>
        <div class="posting-categories">
          <span class="sort-by-location">Remote</span>
        </div>
    """
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[Dict[str, Any]] = []

    for posting in soup.select("div.posting"):
        a = posting.select_one("a.posting-title, a[href]")
        if not a or not a.has_attr("href"):
            continue

        title = (a.get_text() or "").strip()
        href = a["href"]
        url = urljoin(base_url, href)

        loc_el = posting.select_one(".sort-by-location")
        location = (loc_el.get_text() or "").strip() if loc_el else default_location

        # External id: last path segment usually the Lever job id
        external_id = href.rstrip("/").split("/")[-1]

        jobs.append(
            {
                "title": title,
                "company": company_name,
                "company_slug": company_slug,
                "external_id": external_id,
                "url": url,
                "location": location or default_location,
            }
        )

    return jobs


# --------------------------------------------------------------------
# Normalization to Jobs sheet row
# --------------------------------------------------------------------


def _normalize_company_job(job: Dict[str, Any], headers: List[str]) -> Optional[Dict[str, Any]]:
    """
    Convert a scraped job dict into our Jobs sheet row dict.

    Expected input keys:
      - title
      - company
      - company_slug
      - external_id
      - url
      - location
    """
    title = job.get("title") or ""
    company = job.get("company") or ""
    company_slug = job.get("company_slug") or "company"
    external_id = job.get("external_id") or title

    location = job.get("location") or "Remote"

    # classify remote scope; drop if ambiguous
    remote_scope = compute_remote_scope(location)
    if remote_scope not in {"global", "country", "regional"}:
        return None

    source_job_id = _make_source_job_id(company_slug, external_id)
    row_id = f"remotecompany_{source_job_id}"

    tags_list: List[str] = []
    tags_str = ", ".join(tags_list)
    tech_stack_list = extract_tech_stack(tags_list)
    tech_stack_str = ", ".join(tech_stack_list)

    role = normalize_role(title, tags_list)
    category = normalize_category(title, tags_list, role)
    seniority = extract_seniority(title, tags_list)
    # we don't know explicit type – most are full-time
    employment_type = extract_employment_type(title, tags_list)

    # Salary unknown for most career pages
    min_salary_num: Optional[float] = None
    max_salary_num: Optional[float] = None
    currency = "USD"
    min_salary = ""
    max_salary = ""
    high_salary_flag = is_high_salary(min_salary_num, max_salary_num, currency, threshold=HIGH_SALARY_THRESHOLD)

    posted_at = ""  # many boards don't surface this in the listing
    ingested_at = _now_iso()

    row_dict: Dict[str, Any] = {
        "id": row_id,
        "title": title,
        "company": company,
        "source": "RemoteCompanies",
        "url": job.get("url") or "",
        "apply_url": job.get("url") or "",
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

    # Strip any keys not in sheet header
    for key in list(row_dict.keys()):
        if key not in headers:
            row_dict.pop(key, None)

    return row_dict


# --------------------------------------------------------------------
# Main ingest
# --------------------------------------------------------------------


def ingest_remote_companies() -> int:
    """
    Crawl careers pages for remote-first companies and insert new jobs
    into the Jobs sheet. Returns number of rows inserted.
    """
    sheet = get_jobs_sheet()
    headers = _ensure_headers(sheet)

    # Dedupe set: (source, source_job_id)
    existing_records = sheet.get_all_records()
    existing_keys: Set[str] = set()
    for row in existing_records:
        source = _normalize_text(row.get("source"))
        sid = _normalize_text(str(row.get("source_job_id", "")))
        if source and sid:
            existing_keys.add(f"{source}:{sid}")

    logger.info("Loaded %d existing rows from Jobs sheet (for RemoteCompanies ingest)", len(existing_records))

    total_inserted = 0
    all_new_rows: List[List[Any]] = []

    for cfg in REMOTE_COMPANIES:
        slug = cfg["slug"]
        company = cfg["company"]
        ats = cfg["ats"]
        url = cfg["careers_url"]
        default_location = cfg.get("default_location", "Remote")

        logger.info("Fetching careers for %s from %s", company, url)
        html = _fetch_html(url)
        if not html:
            continue

        if ats == "greenhouse":
            scraped_jobs = _parse_greenhouse_board(html, url, slug, company, default_location)
        elif ats == "lever":
            scraped_jobs = _parse_lever_board(html, url, slug, company, default_location)
        else:
            logger.warning("Unknown ATS '%s' for company %s – skipping", ats, company)
            continue

        logger.info("Parsed %d raw jobs for %s (%s)", len(scraped_jobs), company, ats)

        for sj in scraped_jobs:
            external_id = sj.get("external_id") or sj.get("title", "")
            source_job_id = _make_source_job_id(slug, external_id)
            key = f"RemoteCompanies:{source_job_id}"

            if key in existing_keys:
                continue

            row_dict = _normalize_company_job(sj, headers)
            if not row_dict:
                continue

            row_values = [row_dict.get(col, "") for col in headers]
            all_new_rows.append(row_values)
            existing_keys.add(key)
            total_inserted += 1

    if all_new_rows:
        logger.info("Appending %d new RemoteCompanies rows to Jobs sheet", len(all_new_rows))
        sheet.append_rows(all_new_rows, value_input_option="RAW")
    else:
        logger.info("No new RemoteCompanies jobs to insert")

    return total_inserted


if __name__ == "__main__":
    count = ingest_remote_companies()
    print(f"Ingested {count} new RemoteCompanies jobs.")
