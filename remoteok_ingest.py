# remoteok_ingest.py
"""
Fetch jobs from RemoteOK and store them in the Jobs Google Sheet.

- Uses RemoteOK public API: https://remoteok.com/api  (24h delayed feed)
- Maps fields into your Jobs sheet columns:
    id, title, company, source, url, apply_url, source_job_id, location,
    job_roles, job_category, seniority, employment_type,
    tags, tech_stack, min_salary, max_salary, currency,
    high_salary, posted_at, ingested_at, remote_scope
- Skips jobs that already exist (same source + source_job_id)
- NEW: Filters to remote / hybrid only at ingestion time:
    only rows with remote_scope ∈ {global, country, regional} are inserted.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from sheets_client import get_jobs_sheet

load_dotenv()

logger = logging.getLogger("remoteok_ingest")
logger.setLevel(logging.INFO)

REMOTEOK_API_URL = os.environ.get("REMOTEOK_API_URL", "https://remoteok.com/api")
HIGH_SALARY_THRESHOLD = int(os.environ.get("HIGH_SALARY_THRESHOLD", "150000"))

# Known tech keywords (lowercased)
KNOWN_TECH_KEYWORDS = {
    # languages
    "python", "java", "javascript", "typescript", "go", "golang", "ruby", "php",
    "c", "c++", "c#", "scala", "rust", "kotlin", "swift",

    # frameworks / libs
    "django", "flask", "fastapi", "spring", "rails", "laravel",
    "react", "reactjs", "react.js", "nextjs", "next.js", "vue", "vuejs",
    "angular", "svelte", "sveltekit", "nuxt", "nuxtjs",

    # mobile
    "react native", "expo", "swiftui",

    # data / ml
    "sql", "postgres", "postgresql", "mysql", "mongodb", "redis", "elasticsearch",
    "kafka", "spark", "hadoop", "airflow", "dbt", "pandas", "numpy", "pytorch",
    "tensorflow", "sklearn", "scikit-learn", "bigquery", "snowflake", "redshift",

    # devops / infra
    "aws", "gcp", "azure", "docker", "kubernetes", "k8s",
    "terraform", "ansible", "jenkins", "github actions", "gitlab ci",

    # tooling & design
    "git", "linux",
    "figma", "photoshop", "illustrator", "adobe xd",
}

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


# --------------------------------------------------------------------
# Remote / hybrid scope helper
# --------------------------------------------------------------------


def compute_remote_scope(location: str) -> str:
    """
    Classify how 'broad' the remote access is based on location text.

    Returns one of:
    - "global"   -> worldwide / anywhere / global remote
    - "regional" -> region-based (EMEA, LATAM, APAC, Europe, etc.)
    - "country"  -> specific country-level (USA, Canada, UK, Germany, etc.)
    - "onsite"   -> explicitly non-remote/office-only
    - "unknown"  -> anything ambiguous or too specific (city-only, etc.)
    """
    loc = (location or "").strip()
    if not loc:
        return "unknown"

    lower = loc.lower()

    # Onsite / office-only markers
    onsite_markers = [
        "onsite only",
        "on-site only",
        "onsite",
        "on-site",
        "in office",
        "in-office",
        "no remote",
        "not remote",
        "office-based",
    ]
    if any(m in lower for m in onsite_markers):
        return "onsite"

    # Global
    if any(k in lower for k in ["worldwide", "world wide", "anywhere", "global"]):
        return "global"
    if lower in {"remote", "remote only"}:
        return "global"

    # Region-level markers
    region_markers = [
        "emea", "latam", "apac",
        "europe", "asia", "africa",
        "middle east", "south america",
        "north america", "central america",
        "cst +/-", "cet +/-", "gmt+",
        "gmt-", "utc+", "utc-",
    ]
    if any(m in lower for m in region_markers):
        return "regional"

    # If location is a comma-separated list of countries / regions, treat as regional
    if "," in loc:
        return "regional"

    # Country-level heuristics: single-token countries / well-known short codes
    country_tokens = {
        "usa", "us", "united states",
        "canada", "uk", "united kingdom",
        "germany", "france", "spain", "italy",
        "poland", "netherlands", "belgium",
        "sweden", "norway", "denmark", "finland",
        "ireland", "switzerland", "australia",
        "new zealand", "brazil", "mexico",
        "argentina", "chile", "colombia",
        "india", "pakistan", "bangladesh",
        "philippines", "indonesia", "singapore",
        "south africa", "nigeria", "kenya",
        "japan", "south korea",
    }
    if lower in country_tokens:
        return "country"

    # If it looks like "Remote - USA" style
    for c in country_tokens:
        if c in lower:
            return "country"

    return "unknown"


# --------------------------------------------------------------------
# Enrichment helpers
# --------------------------------------------------------------------


def normalize_role(title: str, tags: Any) -> str:
    """
    Map messy titles/tags to a small controlled set of canonical roles.
    """
    text = f"{title} {' '.join(_tags_to_list(tags))}".lower()

    role_patterns = [
        ("Data Scientist", ["data scientist"]),
        ("Machine Learning Engineer", ["ml engineer", "machine learning engineer"]),
        ("Data Engineer", ["data engineer"]),
        ("Data Analyst", ["data analyst", "analytics engineer"]),

        ("DevOps Engineer", ["devops", "site reliability", "sre"]),
        ("Backend Engineer", ["backend engineer", "back-end engineer", "backend developer", "server engineer"]),
        ("Frontend Engineer", ["frontend engineer", "front-end engineer", "frontend developer", "front end developer", "ui engineer"]),
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
        ("Support Specialist", ["customer support", "support specialist", "technical support", "helpdesk", "help desk"]),

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
    """
    Map each job into a broad category from a controlled set.
    """
    if not role:
        role = normalize_role(title, tags)

    role_to_category = {
        # Engineering / Tech
        "Software Engineer": "Engineering",
        "Backend Engineer": "Engineering",
        "Frontend Engineer": "Engineering",
        "Full-Stack Engineer": "Engineering",
        "Mobile Engineer": "Engineering",
        "DevOps Engineer": "Engineering",

        # Data
        "Data Engineer": "Data",
        "Data Scientist": "Data",
        "Data Analyst": "Data",
        "Machine Learning Engineer": "Data",

        # Product / Design
        "Product Manager": "Product",
        "Product Designer": "Design",
        "UX/UI Designer": "Design",

        # GTM
        "Marketing Manager": "Marketing",
        "Growth Marketer": "Marketing",
        "Content Marketer": "Marketing",
        "Sales Representative": "Sales",
        "Account Executive": "Sales",
        "Customer Success Manager": "Customer Support",
        "Support Specialist": "Customer Support",

        # People / Ops
        "Recruiter": "People/HR",
        "HR Generalist": "People/HR",
        "People Operations": "People/HR",
        "Operations Manager": "Operations",
        "Project Manager": "Operations",

        # G&A
        "Finance Manager": "Finance",
        "Accountant": "Finance",
        "Legal Counsel": "Legal",

        # Leadership / Intern
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

    # Default
    return "Mid"


def extract_employment_type(title: str, tags: Any) -> str:
    text = f"{title} {' '.join(_tags_to_list(tags))}".lower()

    if "intern" in text or "internship" in text:
        return "Internship"
    if "part-time" in text or "part time" in text or "parttime" in text:
        return "Part-time"
    if "freelance" in text or "freelancer" in text:
        return "Freelance"
    if "contract" in text or "contractor" in text:
        return "Contract"
    if "temporary" in text or "temp " in text:
        return "Temporary"
    if "full-time" in text or "full time" in text or "fulltime" in text or "permanent" in text:
        return "Full-time"

    # Default assumption for most RemoteOK jobs
    return "Full-time"


def extract_tech_stack(tags: Any) -> List[str]:
    """
    From the tags, return only the "hard-skill" technologies.
    """
    techs: List[str] = []
    seen: Set[str] = set()

    for raw_tag in _tags_to_list(tags):
        tag = raw_tag.strip()
        lower = tag.lower()
        normalized = lower

        # Exact/multi-word match
        if lower in KNOWN_TECH_KEYWORDS:
            if normalized not in seen:
                techs.append(tag)
                seen.add(normalized)
            continue

        # Heuristic: languages with special chars
        if any(ch in tag for ch in ["#", "+", ".NET", ".net"]):
            if normalized not in seen:
                techs.append(tag)
                seen.add(normalized)
            continue

    return techs


def is_high_salary(
    min_salary: Optional[float],
    max_salary: Optional[float],
    currency: Optional[str],
    threshold: int = HIGH_SALARY_THRESHOLD,
) -> bool:
    """
    Simple high_salary flag based on max_salary >= threshold.
    For now we're not currency-normalizing; threshold is assumed to be USD-ish.
    """
    if max_salary is None:
        return False
    try:
        return float(max_salary) >= float(threshold)
    except (ValueError, TypeError):
        return False


# --------------------------------------------------------------------
# RemoteOK-specific helpers
# --------------------------------------------------------------------


def fetch_apply_url(remoteok_url: str) -> str:
    """
    Fetch the RemoteOK job detail page and extract the primary "Apply" link.
    Falls back to the RemoteOK URL on any error or if no apply link found.
    """
    if not remoteok_url:
        return ""

    try:
        headers = {
            "User-Agent": "ASAPJobsBot/1.0 (contact: youremail@example.com)",
            "Accept": "text/html,application/xhtml+xml",
        }
        resp = requests.get(remoteok_url, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        candidates = []

        for a in soup.find_all("a", href=True):
            text = (a.get_text() or "").strip().lower()
            classes = " ".join(a.get("class", [])).lower()
            href = a["href"]

            if "apply" in text or "apply" in classes or "application" in text:
                if href.startswith("/"):
                    href = "https://remoteok.com" + href
                candidates.append(href)

        if candidates:
            return candidates[0]

        return remoteok_url

    except Exception as e:
        logger.warning("Failed to fetch apply_url for %s: %s", remoteok_url, e)
        return remoteok_url


# --------------------------------------------------------------------
# Sheet helpers (headers & dedupe)
# --------------------------------------------------------------------


def _ensure_headers(sheet) -> List[str]:
    """
    Ensure header row exists and that 'apply_url' and 'remote_scope' are present.
    We DO NOT reorder existing columns; if missing, we append at the end.
    """
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
        logger.info("Added missing 'apply_url' column to Jobs sheet")
        updated = True

    if "remote_scope" not in existing:
        headers.append("remote_scope")
        sheet.update_cell(1, len(headers), "remote_scope")
        logger.info("Added missing 'remote_scope' column to Jobs sheet")
        updated = True

    if updated:
        # Re-read in case Google Sheets normalized anything
        headers = sheet.row_values(1)

    return headers


# --------------------------------------------------------------------
# Job normalization
# --------------------------------------------------------------------


def _normalize_remoteok_job(job: Dict[str, Any], headers: List[str]) -> Dict[str, Any] | None:
    """
    Convert one RemoteOK job object into a dict keyed by column name.
    Only returns a row for clearly remote/hybrid jobs
    (remote_scope ∈ {global, country, regional}).
    """
    job_id = job.get("id")
    if not job_id:
        return None

    source_job_id = _to_str(job_id)
    row_id = f"remoteok_{source_job_id}"

    title = job.get("position") or job.get("title") or ""
    company = job.get("company") or ""

    url = job.get("url") or job.get("apply_url") or ""
    if not url and job.get("slug") and source_job_id:
        url = f"https://remoteok.com/remote-jobs/{job['slug']}-{source_job_id}"

    # Location: RemoteOK often has "location" or "region"
    location = job.get("location") or job.get("region") or job.get("country") or "Remote"

    # Classify remote_scope; filter non-remote at ingestion
    remote_scope = compute_remote_scope(location)
    if remote_scope not in {"global", "country", "regional"}:
        # Skip non-remote / onsite / unknown-location jobs
        return None

    # Tags & tech stack
    tags_list = job.get("tags") or []
    tags_list = _tags_to_list(tags_list)
    tags_str = ", ".join(tags_list)
    tech_stack_list = extract_tech_stack(tags_list)
    tech_stack_str = ", ".join(tech_stack_list)

    # Salary info (best-effort; many jobs don’t have these)
    min_salary_num = _parse_float(job.get("salary_min") or job.get("salary_min_usd"))
    max_salary_num = _parse_float(job.get("salary_max") or job.get("salary_max_usd"))
    currency = _to_str(job.get("salary_currency") or "USD")

    min_salary = "" if min_salary_num is None else min_salary_num
    max_salary = "" if max_salary_num is None else max_salary_num

    high_salary_flag = is_high_salary(min_salary_num, max_salary_num, currency)

    # Posted date – RemoteOK uses "date" or "created_at"
    posted_at = _to_str(job.get("date") or job.get("created_at") or "")
    ingested_at = datetime.now(timezone.utc).isoformat()

    role = normalize_role(title, tags_list)
    category = normalize_category(title, tags_list, role)
    seniority = extract_seniority(title, tags_list)
    employment_type = extract_employment_type(title, tags_list)

    apply_url = fetch_apply_url(url) if url else ""

    row_dict: Dict[str, Any] = {
        "id": row_id,
        "title": title,
        "company": company,
        "source": "RemoteOK",
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

    # Strip keys that aren't in headers (in case your sheet has a custom schema)
    for key in list(row_dict.keys()):
        if key not in headers:
            row_dict.pop(key, None)

    return row_dict


# --------------------------------------------------------------------
# Main ingestion pipeline
# --------------------------------------------------------------------


def ingest_remoteok() -> int:
    """
    Fetch RemoteOK jobs and append new ones to the Jobs sheet.
    Returns the number of rows inserted (int).
    Only remote/hybrid jobs are inserted.
    """
    sheet = get_jobs_sheet()
    headers = _ensure_headers(sheet)

    # Existing jobs – dedupe by (source, source_job_id)
    # IMPORTANT: use expected_headers to avoid "header row not unique" error.
    existing_records = sheet.get_all_records(expected_headers=headers)
    existing_keys: Set[str] = set()
    for row in existing_records:
        source = _normalize_text(row.get("source"))
        sid = _normalize_text(str(row.get("source_job_id", "")))
        if source and sid:
            existing_keys.add(f"{source}:{sid}")

    logger.info("Loaded %d existing rows from Jobs sheet", len(existing_records))

    # Call RemoteOK API
    headers_req = {
        "User-Agent": "ASAPJobsBot/1.0 (contact: youremail@example.com)",
    }
    logger.info("Fetching jobs from RemoteOK API: %s", REMOTEOK_API_URL)
    resp = requests.get(REMOTEOK_API_URL, headers=headers_req, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    # RemoteOK returns a list; first element is metadata, then jobs
    jobs: List[Dict[str, Any]] = []
    if isinstance(data, list):
        if data and isinstance(data[0], dict) and "legal" in data[0]:
            jobs = [j for j in data[1:] if isinstance(j, dict) and j.get("id")]
        else:
            jobs = [j for j in data if isinstance(j, dict) and j.get("id")]

    logger.info("Fetched %d jobs from RemoteOK API", len(jobs))

    new_rows: List[List[Any]] = []
    inserted = 0

    for job in jobs:
        source_job_id = _to_str(job.get("id"))
        key = f"RemoteOK:{source_job_id}"

        if key in existing_keys:
            continue  # already stored

        row_dict = _normalize_remoteok_job(job, headers)
        if not row_dict:
            continue  # filtered out (non-remote/hybrid or invalid)

        # Build row in correct column order
        row_values = [row_dict.get(col, "") for col in headers]
        new_rows.append(row_values)
        inserted += 1

    if new_rows:
        logger.info("Appending %d new rows to Jobs sheet", len(new_rows))
        sheet.append_rows(new_rows, value_input_option="RAW")
    else:
        logger.info("No new RemoteOK jobs to insert")

    return inserted


if __name__ == "__main__":
    inserted = ingest_remoteok()
    print(f"Ingested {inserted} new RemoteOK jobs.")
