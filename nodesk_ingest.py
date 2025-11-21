# nodesk_ingest.py
"""
Fetch jobs from NoDesk and store them in the Jobs Google Sheet.

- Scrapes from: https://nodesk.co/remote-jobs/
- Follows links to individual job pages under /remote-jobs/<slug>/
- Skips "collection" / region pages like:
    "Remote Jobs in Europe - NoDesk"
    "Remote Rust Jobs - NoDesk"
  by requiring the page title to look like:
    "<Job Title> at <Company> - NoDesk"

- Maps fields into your Jobs sheet columns:
    id, title, company, source, url, apply_url, source_job_id, location,
    job_roles, job_category, seniority, employment_type,
    tags, tech_stack, min_salary, max_salary, currency,
    high_salary, posted_at, ingested_at, remote_scope

- Skips jobs that already exist (same source + source_job_id)
- Only inserts rows where remote_scope ∈ {global, country, regional}
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from sheets_client import get_jobs_sheet

load_dotenv()

logger = logging.getLogger("nodesk_ingest")
logger.setLevel(logging.INFO)

NODESK_ROOT_URL = "https://nodesk.co"
NODESK_JOBS_URL = f"{NODESK_ROOT_URL}/remote-jobs/"

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
# (copied from your RemoteOK/Remotive logic to keep behavior consistent)
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
        "emea",
        "latam",
        "apac",
        "europe",
        "asia",
        "africa",
        "middle east",
        "south america",
        "north america",
        "central america",
        "cst +/-",
        "cet +/-",
        "gmt+",
        "gmt-",
        "utc+",
        "utc-",
    ]
    if any(m in lower for m in region_markers):
        return "regional"

    # If location is a comma-separated list of countries / regions, treat as regional
    if "," in loc:
        return "regional"

    # Country-level heuristics: single-token countries / well-known short codes
    country_tokens = {
        "usa",
        "us",
        "united states",
        "canada",
        "uk",
        "united kingdom",
        "germany",
        "france",
        "spain",
        "italy",
        "poland",
        "netherlands",
        "belgium",
        "sweden",
        "norway",
        "denmark",
        "finland",
        "ireland",
        "switzerland",
        "australia",
        "new zealand",
        "brazil",
        "mexico",
        "argentina",
        "chile",
        "colombia",
        "india",
        "pakistan",
        "bangladesh",
        "philippines",
        "indonesia",
        "singapore",
        "south africa",
        "nigeria",
        "kenya",
        "japan",
        "south korea",
    }
    if lower in country_tokens:
        return "country"

    # If it looks like "Remote - USA" or similar
    for c in country_tokens:
        if c in lower:
            return "country"

    return "unknown"


# --------------------------------------------------------------------
# Enrichment helpers (copied from RemoteOK so everything stays consistent)
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
        (
            "Backend Engineer",
            [
                "backend engineer",
                "back-end engineer",
                "backend developer",
                "server engineer",
            ],
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
        (
            "Full-Stack Engineer",
            ["fullstack", "full-stack", "full stack"],
        ),
        (
            "Mobile Engineer",
            [
                "mobile engineer",
                "mobile developer",
                "ios engineer",
                "android engineer",
            ],
        ),
        ("Software Engineer", ["software engineer", "software developer", "swe"]),
        ("Product Manager", ["product manager", "product owner"]),
        ("Product Designer", ["product designer"]),
        (
            "UX/UI Designer",
            ["ux designer", "ui designer", "ux/ui", "ux ui"],
        ),
        (
            "Marketing Manager",
            ["marketing manager", "digital marketing manager"],
        ),
        ("Growth Marketer", ["growth marketer", "growth marketing"]),
        (
            "Content Marketer",
            ["content marketer", "content marketing", "copywriter", "copy writer"],
        ),
        (
            "Sales Representative",
            ["sales development", "sdr", "sales representative"],
        ),
        ("Account Executive", ["account executive", "ae"]),
        ("Customer Success Manager", ["customer success", "cs manager"]),
        (
            "Support Specialist",
            [
                "customer support",
                "support specialist",
                "technical support",
                "helpdesk",
                "help desk",
            ],
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
        ("Data", ["data", "analytics",machine learning", "ml"]),
        ("Design", ["designer", "ux", "ui"]),
        ("Product", ["product manager", "product owner"]),
        ("Marketing", ["marketing", "growth", "demand gen", "performance marketing"]),
        ("Sales", ["sales", "account executive", "sdr", "bdr"]),
        (
            "Customer Support",
            ["customer support", "support specialist", "customer success"],
        ),
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

    if any(
        k in text
        for k in ["vp ", "vp,", "vice president", "chief ", " cto", " cfo", " ceo", "coo", "cxo"]
    ):
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
        headers = sheet.row_values(1)

    return headers


# --------------------------------------------------------------------
# NoDesk-specific helpers
# --------------------------------------------------------------------


def _extract_apply_url(soup: BeautifulSoup, fallback_url: str) -> str:
    """
    Find an "Apply" link on the job detail page.
    Falls back to the job page URL if none found.
    """
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().lower()
        if "apply" in text:
            href = a["href"]
            if href.startswith("/"):
                return NODESK_ROOT_URL + href
            return href
    return fallback_url


def _fetch_nodesk_job_links() -> List[str]:
    """
    Fetch the main NoDesk /remote-jobs/ page and collect
    candidate job URLs under /remote-jobs/<slug>/.

    We will still filter later based on the detail page title,
    so this can be a bit generous.
    """
    headers_req = {
        "User-Agent": "ASAPJobsBot/1.0 (contact: youremail@example.com)",
    }
    logger.info("Fetching NoDesk jobs index: %s", NODESK_JOBS_URL)
    resp = requests.get(NODESK_JOBS_URL, headers=headers_req, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    urls: Set[str] = set()

    for a in soup.select("a[href^='/remote-jobs/']"):
        href = a.get("href", "").strip()
        if not href:
            continue

        # Ignore obvious pagination or non-job patterns if any
        # (we rely mainly on detail-page filters later)
        if href.startswith("/remote-jobs/page/"):
            continue

        # Build absolute URL, strip anchors and trailing slash
        if href.startswith("http"):
            full = href
        else:
            full = NODESK_ROOT_URL + href

        full = full.split("#")[0].rstrip("/")
        urls.add(full)

    logger.info("Collected %d candidate NoDesk job URLs from index", len(urls))
    return sorted(urls)


def _normalize_nodesk_job(job_url: str, headers: List[str]) -> Optional[Dict[str, Any]]:
    """
    Load one NoDesk job detail page and convert it into a row dict.

    IMPORTANT:
    - Only treat pages as jobs if their <title> looks like:
        "<Job Title> at <Company> - NoDesk"
      i.e. contains " at " and ends (optionally) with " - NoDesk".
    - This automatically skips "Remote Jobs in X" and collection pages.
    """
    headers_req = {
        "User-Agent": "ASAPJobsBot/1.0 (contact: youremail@example.com)",
    }

    try:
        resp = requests.get(job_url, headers=headers_req, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Failed to fetch NoDesk job page %s: %s", job_url, e)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    title_tag = soup.find("title")
    if not title_tag:
        logger.debug("No <title> for %s, skipping", job_url)
        return None

    full_title = title_tag.get_text(strip=True)

    # Example job page: "Customer Success Manager at BuzzyBooth - NoDesk"
    # Example listing page: "Remote Jobs in Africa - NoDesk"
    if " at " not in full_title:
        # Not a single job; very likely a category/region/filter page.
        logger.debug("Skipping NoDesk non-job page (no ' at ' in title): %s", full_title)
        return None

    # Strip the trailing " - NoDesk" branding if present
    cleaned = full_title.replace("– NoDesk", "").replace("- NoDesk", "").strip()

    try:
        title_part, company_part = cleaned.split(" at ", 1)
    except ValueError:
        logger.debug("Title doesn't split cleanly on ' at ' for %s -> %s", job_url, cleaned)
        return None

    title = title_part.strip()
    company = company_part.strip()

    # Location: best-effort capture of something like "Remote - Worldwide", "Remote - USA", etc.
    location = ""
    for tag in soup.find_all(["p", "span", "li"]):
        text = (tag.get_text(" ", strip=True) or "").strip()
        if text.startswith("Remote") and len(text) <= 80:
            # e.g. "Remote - Worldwide" / "Remote - USA" / "Remote - LATAM"
            location = text
            break

    if not location:
        # Fallback: if the title starts with 'Remote ' we still treat it as remote.
        if cleaned.lower().startswith("remote "):
            location = "Remote"
        else:
            location = "Remote"

    remote_scope = compute_remote_scope(location)
    if remote_scope not in {"global", "country", "regional"}:
        # We only want broad-remote roles; skip overly specific or onsite roles.
        logger.debug(
            "Skipping NoDesk job %s with narrow/unknown remote_scope '%s' (location: %s)",
            job_url,
            remote_scope,
            location,
        )
        return None

    # Tags: NoDesk often shows things like "Worldwide, Sales" etc.
    # We keep this simple; you can evolve this later if you want richer tags.
    tags_list: List[str] = []
    tags_str = ""

    tech_stack_str = ""

    # NoDesk rarely exposes salary in a structured way; leave blank for now.
    min_salary = ""
    max_salary = ""
    currency = "USD"
    high_salary_flag = False

    posted_at = ""  # Not easily/consistently available from pages.
    ingested_at = datetime.now(timezone.utc).isoformat()

    role = normalize_role(title, tags_list)
    category = normalize_category(title, tags_list, role)
    seniority = extract_seniority(title, tags_list)
    employment_type = extract_employment_type(title, tags_list)

    apply_url = _extract_apply_url(soup, job_url)

    source_job_id = job_url.rstrip("/").split("/")[-1] or job_url
    row_id = f"nodesk_{source_job_id}"

    row_dict: Dict[str, Any] = {
        "id": row_id,
        "title": title,
        "company": company,
        "source": "NoDesk",
        "url": job_url,
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


def ingest_nodesk() -> int:
    """
    Scrape NoDesk remote jobs and append new ones to the Jobs sheet.
    Returns the number of rows inserted (int).
    Only broad-remote jobs are inserted (remote_scope ∈ {global, country, regional}).
    """
    sheet = get_jobs_sheet()
    headers = _ensure_headers(sheet)

    # Build existing key set (source:source_job_id) WITHOUT get_all_records()
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
        for row in all_values[1:]:  # skip header row
            src = _normalize_text(row[idx_source]) if idx_source < len(row) else ""
            sid = _normalize_text(row[idx_sid]) if idx_sid < len(row) else ""
            if src and sid:
                existing_keys.add(f"{src}:{sid}")

    logger.info(
        "Loaded %d existing rows from Jobs sheet (NoDesk dedupe)",
        len(all_values) - 1 if all_values else 0,
    )

    job_urls = _fetch_nodesk_job_links()
    logger.info("Processing %d NoDesk candidate job URLs", len(job_urls))

    new_rows: List[List[Any]] = []
    inserted = 0

    for job_url in job_urls:
        source_job_id = job_url.rstrip("/").split("/")[-1] or job_url
        key = f"NoDesk:{source_job_id}"
        if key in existing_keys:
            continue  # already stored

        row_dict = _normalize_nodesk_job(job_url, headers)
        if not row_dict:
            continue  # filtered out or invalid

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
