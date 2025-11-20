# nodesk_ingest.py
"""
Fetch remote-first jobs from NoDesk and store them in the Jobs Google Sheet.

- Scrapes the main remote jobs page (remote-first jobs):
    Default: https://nodesk.co/remote-jobs/remote-first/
    (You can override with NODESK_JOBS_URL env var, e.g. https://nodesk.co/remote-jobs/)

- Maps fields into your Jobs sheet columns:
    id, title, company, source, url, apply_url, source_job_id, location,
    job_roles, job_category, seniority, employment_type,
    tags, tech_stack, min_salary, max_salary, currency,
    high_salary, posted_at, ingested_at, remote_scope

- Skips jobs that already exist (same source + source_job_id)
- Filters to clearly remote jobs by using remote_scope:
    only rows with remote_scope ∈ {global, country, regional} are inserted.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from sheets_client import get_jobs_sheet

load_dotenv()

logger = logging.getLogger("nodesk_ingest")
logger.setLevel(logging.INFO)

NODESK_JOBS_URL = os.environ.get(
    "NODESK_JOBS_URL",
    # This is the main "Remote-First Jobs" page.
    "https://nodesk.co/remote-jobs/remote-first/",
)

HIGH_SALARY_THRESHOLD = int(os.environ.get("HIGH_SALARY_THRESHOLD", "150000"))

# --------------------------------------------------------------------
# Shared helpers (same style as remoteok/remotive)
# --------------------------------------------------------------------


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def _normalize_text(s: Optional[str]) -> str:
    return (s or "").strip()


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def _tags_to_list(tags: Any) -> List[str]:
    if tags is None:
        return []
    if isinstance(tags, list):
        return [str(t).strip() for t in tags if str(t).strip()]
    if isinstance(tags, str):
        return [t.strip() for t in tags.split(",") if t.strip()]
    return [str(tags).strip()]


# Known tech keywords (lowercased)
KNOWN_TECH_KEYWORDS = {
    # languages
    "python", "java", "javascript", "typescript", "go", "golang", "ruby", "php",
    "c", "c++", "c#", "scala", "rust", "kotlin", "swift",

    # frameworks / libs
    "django", "flask", "fastapi", "spring", "rails", "laravel",
    "react", "reactjs", "react.js", "nextjs", "next.js", "vue", "vuejs",
    "angular", "svelte", "sveltekit", "nuxt", "nuxtjs",

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

    for c in country_tokens:
        if c in lower:
            return "country"

    return "unknown"


def normalize_role(title: str, tags: Any) -> str:
    """
    Map messy titles/tags to a small controlled set of canonical roles.
    Same mapping as remoteok_ingest.
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

    return "Full-time"


def extract_tech_stack(tags: Any) -> List[str]:
    techs: List[str] = []
    seen: Set[str] = set()

    for raw_tag in _tags_to_list(tags):
        tag = raw_tag.strip()
        lower = tag.lower()
        normalized = lower

        if lower in KNOWN_TECH_KEYWORDS:
            if normalized not in seen:
                techs.append(tag)
                seen.add(normalized)
            continue

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
    if max_salary is None:
        return False
    try:
        return float(max_salary) >= float(threshold)
    except (ValueError, TypeError):
        return False


# --------------------------------------------------------------------
# Sheet helpers
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
        headers = sheet.row_values(1)

    return headers


# --------------------------------------------------------------------
# NoDesk-specific parsing
# --------------------------------------------------------------------


def _extract_source_job_id(url: str) -> str:
    """
    Use the last path segment of the job URL as a stable source_job_id.
    """
    try:
        path = urlparse(url).path.rstrip("/")
        slug = path.split("/")[-1]
        return slug or path or url
    except Exception:
        return url


def _normalize_nodesk_job(h2_tag, headers: List[str]) -> Optional[Dict[str, Any]]:
    """
    Convert one NoDesk job block (starting at an <h2>) into a row dict.

    We keep parsing simple and robust:
    - title from <h2><a>
    - company from the first following <h3>
    - location from the 'Remote:' line + its following element (e.g. 'Worldwide')
    - tags are minimal (category + employment type text if available)
    - salary and posted_at left empty for now (NoDesk shows relative times)
    """
    a = h2_tag.find("a", href=True)
    if not a:
        return None

    href = a["href"]
    if not href:
        return None

    url = href
    if not url.startswith("http"):
        url = urljoin("https://nodesk.co", url)

    # Only keep real job detail pages, not collections or ads
    if "/remote-jobs/collections" in url:
        return None
    if "/remote-jobs/" not in url:
        return None

    title = a.get_text(strip=True)
    if not title:
        return None

    container = h2_tag.parent

    # Company is usually the next <h3>
    company_tag = container.find("h3")
    company = company_tag.get_text(strip=True) if company_tag else ""

    # Find Remote: <something>
    location = "Remote"
    remote_location = ""

    remote_label = None
    for tag in container.find_all(["h4", "p", "span"]):
        text = (tag.get_text() or "").strip()
        if text.lower().startswith("remote:"):
            remote_label = tag
            break

    if remote_label:
        # The next element that isn't just "Remote:" is the location, e.g. "Worldwide"
        loc_tag = remote_label.find_next()
        # Skip the Remote: label itself
        while loc_tag and (loc_tag == remote_label or not loc_tag.get_text(strip=True)):
            loc_tag = loc_tag.find_next()
        if loc_tag:
            remote_location = loc_tag.get_text(strip=True)

    if remote_location:
        # e.g. "Worldwide", "UTC-7 to UTC+1", "Europe", etc.
        if "remote" in remote_location.lower():
            location = remote_location
        else:
            location = f"Remote - {remote_location}"
    else:
        location = "Remote"

    remote_scope = compute_remote_scope(location)
    if remote_scope not in {"global", "country", "regional"}:
        # We only ingest clearly remote jobs
        return None

    # Minimal tags: job category + employment type text if we can find them
    category_text = ""
    emp_text = ""

    # After the location, we usually have:
    # - Category (e.g. "Sales", "Engineering")
    # - Employment type ("Full-Time", "Part-Time", "Contract", etc.)
    # We'll try to read the next two "heading-ish" elements.
    heading_tags = []
    for tag in container.find_all(["h4", "h5"]):
        heading_tags.append(tag)

    # Try to locate indices relative to remote_label
    if remote_label and heading_tags:
        try:
            idx = heading_tags.index(remote_label)
        except ValueError:
            idx = -1

        # Next heading-ish tag after 'Remote:'
        if 0 <= idx + 1 < len(heading_tags):
            category_text = heading_tags[idx + 1].get_text(strip=True)
        # Next one after that is often employment type
        if 0 <= idx + 2 < len(heading_tags):
            emp_text = heading_tags[idx + 2].get_text(strip=True)

    extra_tags: List[str] = []
    if category_text:
        extra_tags.append(category_text)
    if emp_text:
        extra_tags.append(emp_text)

    tags_str = ", ".join(extra_tags)
    tech_stack_list = extract_tech_stack(extra_tags)
    tech_stack_str = ", ".join(tech_stack_list)

    # Role/category/seniority/employment_type via shared helpers
    role = normalize_role(title, extra_tags)
    category = normalize_category(title, extra_tags, role)
    seniority = extract_seniority(title, extra_tags)

    employment_type = ""
    if emp_text:
        employment_type = extract_employment_type(title, [emp_text])
    else:
        employment_type = extract_employment_type(title, extra_tags)

    # Salary & currency – NoDesk shows "$80K – $100K" etc but we keep it simple for now.
    min_salary_num = None
    max_salary_num = None
    currency = ""

    min_salary = "" if min_salary_num is None else min_salary_num
    max_salary = "" if max_salary_num is None else max_salary_num
    high_salary_flag = is_high_salary(min_salary_num, max_salary_num, currency or "USD")

    # Posted date – NoDesk shows relative like "1d", "3d"
    posted_at = ""
    ingested_at = datetime.now(timezone.utc).isoformat()

    source_job_id = _extract_source_job_id(url)
    row_id = f"nodesk_{source_job_id}"

    row_dict: Dict[str, Any] = {
        "id": row_id,
        "title": title,
        "company": company,
        "source": "NoDesk",
        "url": url,
        "apply_url": url,  # we send users to the NoDesk job page; they can apply from there
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

    # Keep only known headers
    for key in list(row_dict.keys()):
        if key not in headers:
            row_dict.pop(key, None)

    return row_dict


# --------------------------------------------------------------------
# Main ingestion pipeline
# --------------------------------------------------------------------


def ingest_nodesk() -> int:
    """
    Fetch NoDesk remote-first jobs and append new ones to the Jobs sheet.
    Returns the number of rows inserted (int).
    """
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
        "Loaded %d existing rows from Jobs sheet (NoDesk dedupe)",
        len(all_values) - 1 if all_values else 0,
    )

    # Fetch NoDesk remote-first jobs page
    headers_req = {
        "User-Agent": "ASAPJobsBot/1.0 (contact: youremail@example.com)",
    }
    logger.info("Fetching jobs from NoDesk: %s", NODESK_JOBS_URL)
    resp = requests.get(NODESK_JOBS_URL, headers=headers_req, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    new_rows: List[List[Any]] = []
    inserted = 0

    # Each job is in an <h2> with a link to /remote-jobs/<slug>/
    h2_tags = soup.find_all("h2")
    logger.info("Found %d <h2> tags on NoDesk page", len(h2_tags))

    for h2 in h2_tags:
        a = h2.find("a", href=True)
        if not a:
            continue

        href = a["href"]
        if not href:
            continue

        url = href
        if not url.startswith("http"):
            url = urljoin("https://nodesk.co", url)

        # Only job detail pages
        if "/remote-jobs/collections" in url:
            continue
        if "/remote-jobs/" not in url:
            continue

        source_job_id = _extract_source_job_id(url)
        key = f"NoDesk:{source_job_id}"
        if key in existing_keys:
            continue

        row_dict = _normalize_nodesk_job(h2, headers)
        if not row_dict:
            continue

        row_values = [row_dict.get(col, "") for col in headers]
        new_rows.append(row_values)
        inserted += 1

    if new_rows:
        logger.info("Appending %d new NoDesk rows to Jobs sheet", len(new_rows))
        sheet.append_rows(new_rows, value_input_option="RAW")
    else:
        logger.info("No new NoDesk jobs to insert")

    return inserted


if __name__ == "__main__":
    inserted = ingest_nodesk()
    print(f"Ingested {inserted} new NoDesk jobs.")
