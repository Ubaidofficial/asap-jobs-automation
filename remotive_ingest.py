# remotive_ingest.py
"""
Fetch jobs from Remotive and store them in the Jobs Google Sheet.

Remotive is a remote-only job board, so we treat all jobs as remote.
We still classify how broad the remote access is via `remote_scope`:
    - global   -> Worldwide / anywhere
    - regional -> Europe, LATAM, APAC, etc.
    - country  -> USA, Canada, India, etc.
    - onsite   -> only if explicitly marked on-site / office / hybrid

Columns written (must exist in Jobs sheet, order doesn’t matter):
  id, title, company, source, url, apply_url, source_job_id, location,
  job_roles, job_category, seniority, employment_type,
  tags, tech_stack, min_salary, max_salary, currency,
  high_salary, posted_at, ingested_at, remote_scope
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import requests

from sheets_client import get_jobs_sheet

logger = logging.getLogger("remotive_ingest")
logger.setLevel(logging.INFO)

REMOTIVE_API_URL = os.environ.get(
    "REMOTIVE_API_URL", "https://remotive.com/api/remote-jobs"
)
HIGH_SALARY_THRESHOLD = int(os.environ.get("HIGH_SALARY_THRESHOLD", "150000"))

# --------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def _normalize_text(s: Optional[str]) -> str:
    return (s or "").strip()


def _tags_to_list(tags: Any) -> List[str]:
    if tags is None:
        return []
    if isinstance(tags, list):
        return [str(t).strip() for t in tags if str(t).strip()]
    if isinstance(tags, str):
        return [t.strip() for t in tags.split(",") if t.strip()]
    return [str(tags).strip()]


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def is_high_salary(
    min_salary: Optional[float],
    max_salary: Optional[float],
    currency: Optional[str],
    threshold: int = HIGH_SALARY_THRESHOLD,
) -> bool:
    if max_salary is None:
        return False
    try:
        return float(max_salary) >= float(threshold)
    except (ValueError, TypeError):
        return False


# --------------------------------------------------------------------
# Role / category / seniority mapping
# (same style as remoteok_ingest for consistency)
# --------------------------------------------------------------------


def normalize_role(title: str, tags: Any) -> str:
    text = f"{title} {' '.join(_tags_to_list(tags))}".lower()

    role_patterns = [
        ("Data Scientist", ["data scientist"]),
        ("Machine Learning Engineer", ["ml engineer", "machine learning engineer"]),
        ("Data Engineer", ["data engineer"]),
        ("Data Analyst", ["data analyst", "analytics engineer"]),
        ("DevOps Engineer", ["devops", "site reliability", "sre"]),
        (
            "Backend Engineer",
            ["backend engineer", "back-end engineer", "backend developer", "server engineer"],
        ),
        (
            "Frontend Engineer",
            [
                "frontend engineer",
                "front-end engineer",
                "frontend developer",
                "front end developer",
                "ui engineer",
            ],
        ),
        ("Full-Stack Engineer", ["fullstack", "full-stack", "full stack"]),
        ("Mobile Engineer", ["mobile engineer", "mobile developer", "ios engineer", "android engineer"]),
        ("Software Engineer", ["software engineer", "software developer", "swe"]),
        ("Product Manager", ["product manager", "product owner"]),
        ("Product Designer", ["product designer"]),
        ("UX/UI Designer", ["ux designer", "ui designer", "ux/ui", "ux ui"]),
        ("Marketing Manager", ["marketing manager", "digital marketing manager"]),
        ("Growth Marketer", ["growth marketer", "growth marketing"]),
        ("Content Marketer", ["content marketer", "content marketing", "copywriter", "copy writer"]),
        ("Sales Representative", ["sales development", "sdr", "sales representative"]),
        ("Account Executive", ["account executive", "ae"]),
        ("Customer Success Manager", ["customer success", "cs manager"]),
        (
            "Support Specialist",
            ["customer support", "support specialist", "technical support", "helpdesk", "help desk"],
        ),
        ("Recruiter", ["recruiter", "talent acquisition"]),
        ("HR Generalist", ["hr generalist"]),
        ("People Operations", ["people ops", "people operations"]),
        ("Operations Manager", ["operations manager", "ops manager", "business operations"]),
        ("Project Manager", ["project manager", "program manager"]),
        ("Finance Manager", ["finance manager", "fp&a", "financial analyst"]),
        ("Accountant", ["accountant"]),
        ("Legal Counsel", ["legal counsel", "attorney", "lawyer"]),
        ("Founder / CEO", ["founder", "co-founder", "ceo"]),
        ("CTO", ["cto", "chief technology officer"]),
        ("COO", ["coo", "chief operating officer"]),
        ("CPO", ["cpo", "chief product officer"]),
        ("Intern", ["intern", "internship"]),
    ]

    for canonical, patterns in role_patterns:
        for p in patterns:
            if p in text:
                return canonical

    return "Other"


def normalize_category(title: str, tags: Any, role: Optional[str] = None) -> str:
    if not role:
        role = normalize_role(title, tags)

    role_to_category = {
        "Software Engineer": "Engineering",
        "Backend Engineer": "Engineering",
        "Frontend Engineer": "Engineering",
        "Full-Stack Engineer": "Engineering",
        "Mobile Engineer": "Engineering",
        "DevOps Engineer": "Engineering",
        "Data Engineer": "Data",
        "Data Scientist": "Data",
        "Data Analyst": "Data",
        "Machine Learning Engineer": "Data",
        "Product Manager": "Product",
        "Product Designer": "Design",
        "UX/UI Designer": "Design",
        "Marketing Manager": "Marketing",
        "Growth Marketer": "Marketing",
        "Content Marketer": "Marketing",
        "Sales Representative": "Sales",
        "Account Executive": "Sales",
        "Customer Success Manager": "Customer Support",
        "Support Specialist": "Customer Support",
        "Recruiter": "People/HR",
        "HR Generalist": "People/HR",
        "People Operations": "People/HR",
        "Operations Manager": "Operations",
        "Project Manager": "Operations",
        "Finance Manager": "Finance",
        "Accountant": "Finance",
        "Legal Counsel": "Legal",
        "Founder / CEO": "Leadership",
        "CTO": "Leadership",
        "COO": "Leadership",
        "CPO": "Leadership",
        "Intern": "Internship",
    }

    if role in role_to_category:
        return role_to_category[role]

    text = f"{title} {' '.join(_tags_to_list(tags))}".lower()

    keyword_category = [
        ("Engineering", ["engineer", "developer", "devops", "sre"]),
        ("Data", ["data", "analytics", "machine learning", "ml"]),
        ("Design", ["designer", "ux", "ui"]),
        ("Product", ["product manager", "product owner"]),
        ("Marketing", ["marketing", "growth", "demand gen", "performance marketing"]),
        ("Sales", ["sales", "account executive", "sdr", "bdr"]),
        ("Customer Support", ["customer support", "support specialist", "customer success"]),
        ("People/HR", ["recruiter", "talent", "hr", "people ops"]),
        ("Operations", ["operations", "ops manager", "program manager"]),
        ("Finance", ["finance", "accountant", "fp&a"]),
        ("Legal", ["legal", "counsel", "attorney"]),
        ("Leadership", ["head of", "vp", "vice president", "chief", "c-level", "cxo"]),
        ("Internship", ["intern", "internship"]),
    ]

    for cat, patterns in keyword_category:
        for p in patterns:
            if p in text:
                return cat

    return "Other"


def extract_seniority(title: str, tags: Any) -> str:
    text = f"{title} {' '.join(_tags_to_list(tags))}".lower()

    if any(k in text for k in ["vp ", "vp,", "vice president", "chief ", " cto", " cfo", " ceo", "coo", "cxo"]):
        return "VP/C-level"
    if "head of" in text:
        return "Director"
    if "director" in text:
        return "Director"
    if "principal" in text:
        return "Principal"
    if "staff" in text:
        return "Staff"
    if "lead" in text:
        return "Lead"
    if "senior" in text or " sr " in text or " sr." in text or "sr " in text:
        return "Senior"
    if "junior" in text or " jr " in text or " jr." in text:
        return "Junior"
    if "intern" in text or "internship" in text:
        return "Intern"
    return "Mid"


def extract_employment_type(title: str, tags: Any) -> str:
    text = f"{title} {' '.join(_tags_to_list(tags))}".lower()

    if "intern" in text or "internship" in text:
        return "Internship"
    if "part-time" in text or "part time" in text or "parttime" in text:
        return "Part-time"
    if "freelance" in text or "freelancer" in text or "contract" in text or "contractor" in text:
        # A lot of Remotive postings mark contract/freelance together
        if "contract" in text or "contractor" in text:
            return "Contract"
        return "Freelance"
    if "temporary" in text or "temp " in text:
        return "Temporary"
    if "full-time" in text or "full time" in text or "fulltime" in text or "permanent" in text:
        return "Full-time"
    return "Full-time"


# --------------------------------------------------------------------
# Remote scope for Remotive
# --------------------------------------------------------------------


def compute_remotive_scope_and_location(raw: str) -> (str, str):
    """
    Remotive's `candidate_required_location` describes WHERE the remote worker
    may be located (e.g. 'Worldwide', 'Europe', 'USA Only', 'LATAM', 'India').

    We always treat these as remote, and return (remote_scope, nice_location_label).
    """
    text = (raw or "").strip()
    lower = text.lower()

    # Explicit onsite / hybrid markers (rare on Remotive)
    onsite_markers = ["onsite", "on-site", "office", "hybrid"]
    if any(m in lower for m in onsite_markers):
        return "onsite", text or "Onsite"

    # Default labels if missing
    if not text:
        return "global", "Remote - Worldwide"

    # Global
    if any(k in lower for k in ["worldwide", "anywhere", "global"]):
        return "global", "Remote - Worldwide"

    # Region-level
    region_keywords = [
        "europe",
        "emea",
        "latam",
        "apac",
        "asia",
        "africa",
        "middle east",
        "north america",
        "south america",
        "central america",
        "australia, new zealand",
        "australia/new zealand",
        "oceania",
        "eu",
        "americas",
    ]
    if any(k in lower for k in region_keywords):
        return "regional", f"Remote - {text}"

    # Multi-country strings like "USA, Canada"
    if "," in text:
        return "regional", f"Remote - {text}"

    # Country-level
    return "country", f"Remote - {text}"


# --------------------------------------------------------------------
# Sheet helpers
# --------------------------------------------------------------------


def _ensure_headers(sheet) -> List[str]:
    headers = sheet.row_values(1)
    if not headers:
        headers = [
            "id",
            "title",
            "company",
            "source",
            "url",
            "apply_url",
            "source_job_id",
            "location",
            "job_roles",
            "job_category",
            "seniority",
            "employment_type",
            "tags",
            "tech_stack",
            "min_salary",
            "max_salary",
            "currency",
            "high_salary",
            "posted_at",
            "ingested_at",
            "remote_scope",
        ]
        sheet.insert_row(headers, 1)
        return headers

    existing = {h.strip() for h in headers if h}
    updated = False

    if "apply_url" not in existing:
        headers.append("apply_url")
        sheet.update_cell(1, len(headers), "apply_url")
        updated = True

    if "remote_scope" not in existing:
        headers.append("remote_scope")
        sheet.update_cell(1, len(headers), "remote_scope")
        updated = True

    if updated:
        headers = sheet.row_values(1)

    return headers


# --------------------------------------------------------------------
# Job normalization
# --------------------------------------------------------------------


def _normalize_remotive_job(job: Dict[str, Any], headers: List[str]) -> Optional[Dict[str, Any]]:
    job_id = job.get("id")
    if not job_id:
        return None

    source_job_id = _to_str(job_id)
    row_id = f"remotive_{source_job_id}"

    title = job.get("title") or ""
    company = job.get("company_name") or ""
    url = job.get("url") or ""

    # Location handling – Remotive specific
    candidate_loc = job.get("candidate_required_location") or ""
    remote_scope, nice_location = compute_remotive_scope_and_location(candidate_loc)

    tags_list = job.get("tags") or []
    tags_list = _tags_to_list(tags_list)
    tags_str = ", ".join(tags_list)

    # Remotive has salary in a few different fields, we just pick up min/max if present
    min_salary_num = _parse_float(job.get("salary_min"))
    max_salary_num = _parse_float(job.get("salary_max"))
    currency = _to_str(job.get("salary_currency") or "USD")

    min_salary = "" if min_salary_num is None else min_salary_num
    max_salary = "" if max_salary_num is None else max_salary_num
    high_salary_flag = is_high_salary(min_salary_num, max_salary_num, currency)

    posted_at = _to_str(job.get("publication_date") or "")
    ingested_at = datetime.now(timezone.utc).isoformat()

    role = normalize_role(title, tags_list)
    category = normalize_category(title, tags_list, role)
    seniority = extract_seniority(title, tags_list)
    employment_type = extract_employment_type(title, tags_list)

    row: Dict[str, Any] = {
        "id": row_id,
        "title": title,
        "company": company,
        "source": "Remotive",
        "url": url,
        "apply_url": url,  # Remotive URL is also the apply target
        "source_job_id": source_job_id,
        "location": nice_location,
        "job_roles": role,
        "job_category": category,
        "seniority": seniority,
        "employment_type": employment_type,
        "tags": tags_str,
        "tech_stack": "",  # could be derived from tags later
        "min_salary": min_salary,
        "max_salary": max_salary,
        "currency": currency,
        "high_salary": "TRUE" if high_salary_flag else "FALSE",
        "posted_at": posted_at,
        "ingested_at": ingested_at,
        "remote_scope": remote_scope,
    }

    # Strip unknown columns
    for key in list(row.keys()):
        if key not in headers:
            row.pop(key, None)

    return row


# --------------------------------------------------------------------
# Main ingestion pipeline
# --------------------------------------------------------------------


def ingest_remotive() -> int:
    sheet = get_jobs_sheet()
    headers = _ensure_headers(sheet)

    # Build existing key set (source:source_job_id)
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
        for row in all_values[1:]:
            src = _normalize_text(row[idx_source]) if idx_source < len(row) else ""
            sid = _normalize_text(row[idx_sid]) if idx_sid < len(row) else ""
            if src and sid:
                existing_keys.add(f"{src}:{sid}")

    logger.info(
        "Loaded %d existing rows from Jobs sheet (Remotive dedupe)",
        len(all_values) - 1 if all_values else 0,
    )

    logger.info("Fetching jobs from Remotive API: %s", REMOTIVE_API_URL)
    resp = requests.get(REMOTIVE_API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    jobs_list = data.get("jobs", []) if isinstance(data, dict) else []
    logger.info("Fetched %d jobs from Remotive API", len(jobs_list))

    new_rows: List[List[Any]] = []
    inserted = 0

    for job in jobs_list:
        if not isinstance(job, dict):
            continue

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
    inserted = ingest_remotive()
    print(f"Ingested {inserted} new Remotive jobs.")
