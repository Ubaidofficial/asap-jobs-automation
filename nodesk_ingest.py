# nodesk_ingest.py
"""
Fetch jobs from NoDesk and store them in the Jobs Google Sheet.

- Scrapes https://nodesk.co/remote-jobs/
- Follows individual job pages at /remote-jobs/<slug>/
- Skips:
    * /remote-jobs/collections/
    * /remote-jobs/remote-first/<category>/
    * /remote-jobs/tags/<tag>/
- Maps fields into your Jobs sheet columns:
    id, title, company, source, url, source_job_id, location,
    job_roles, job_category, seniority, employment_type,
    tags, tech_stack, min_salary, max_salary, currency,
    high_salary, posted_at, ingested_at, apply_url, remote_scope
- Skips non-remote / ambiguous jobs:
    only rows with remote_scope ∈ {global, regional, country} are inserted.
"""

import os
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

NODESK_BASE_URL = "https://nodesk.co"
NODESK_JOBS_URL = f"{NODESK_BASE_URL}/remote-jobs/"
HIGH_SALARY_THRESHOLD = int(os.environ.get("HIGH_SALARY_THRESHOLD", "150000"))

USER_AGENT = "ASAPJobsBot/1.0 (contact: youremail@example.com)"

# --------------------------------------------------------------------
# Generic helpers (same style as remoteok_ingest / remotive_ingest)
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


def compute_remote_scope(location: str) -> str:
    """
    Classify how 'broad' the remote access is based on location text.

    Returns one of:
    - "global"   -> worldwide / anywhere / global remote
    - "regional" -> region-based (EMEA, LATAM, APAC, Europe, etc.)
    - "country"  -> specific country-level (USA, Canada, UK, etc.)
    - "onsite"   -> explicitly non-remote/office-only
    - "unknown"  -> anything ambiguous or too specific (city-only, etc.)
    """
    loc = (location or "").strip()
    if not loc:
        return "unknown"

    lower = loc.lower()

    # If it doesn't even mention "remote", treat as onsite
    if "remote" not in lower:
        return "onsite"

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
        "americas",
        "cst +/-",
        "cet +/-",
        "gmt+",
        "gmt-",
        "utc+",
        "utc-",
    ]
    if any(m in lower for m in region_markers):
        return "regional"

    # If location is a comma/• separated list of countries / regions, treat as regional
    if "," in loc or "·" in loc or "|" in loc:
        return "regional"

    # Country-level heuristics
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

    for c in country_tokens:
        if c in lower:
            return "country"

    return "unknown"


# --- very light role/category/seniority/type helpers (same spirit as other ingests)


def normalize_role(title: str, tags: Any) -> str:
    text = f"{title} {' '.join(_tags_to_list(tags))}".lower()

    role_patterns = [
        ("Data Scientist", ["data scientist"]),
        ("Machine Learning Engineer", ["ml engineer", "machine learning engineer"]),
        ("Data Engineer", ["data engineer"]),
        ("Data Analyst", ["data analyst", "analytics engineer"]),
        ("DevOps Engineer", ["devops", "site reliability", "sre"]),
        ("Backend Engineer", ["backend engineer", "backend developer", "back-end"]),
        ("Frontend Engineer", ["frontend engineer", "frontend developer", "front-end"]),
        ("Full-Stack Engineer", ["fullstack", "full-stack", "full stack"]),
        ("Mobile Engineer", ["mobile engineer", "ios engineer", "android engineer"]),
        ("Software Engineer", ["software engineer", "software developer"]),
        ("Product Manager", ["product manager", "product owner"]),
        ("Product Designer", ["product designer"]),
        ("UX/UI Designer", ["ux designer", "ui designer", "ux/ui"]),
        ("Marketing Manager", ["marketing manager"]),
        ("Growth Marketer", ["growth marketing", "growth marketer"]),
        ("Content Marketer", ["content marketing", "copywriter"]),
        ("Sales Representative", ["sales development", "sdr", "sales representative"]),
        ("Account Executive", ["account executive", "ae"]),
        ("Customer Success Manager", ["customer success"]),
        ("Support Specialist", ["customer support", "support specialist"]),
        ("Recruiter", ["recruiter", "talent acquisition"]),
        ("Operations Manager", ["operations manager", "ops manager"]),
        ("Project Manager", ["project manager", "program manager"]),
        ("Finance Manager", ["finance manager"]),
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
        ("Marketing", ["marketing", "growth", "demand gen"]),
        ("Sales", ["sales", "account executive", "sdr", "bdr"]),
        ("Customer Support", ["customer support", "customer success"]),
        ("People/HR", ["recruiter", "talent", "hr", "people ops"]),
        ("Operations", ["operations", "ops manager", "program manager"]),
        ("Finance", ["finance", "accountant", "fp&a"]),
        ("Legal", ["legal", "counsel", "attorney"]),
        ("Leadership", ["head of", "vp", "vice president", "chief"]),
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
# NoDesk specific helpers
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
            "apply_url",
            "remote_scope",
        ]
        sheet.insert_row(headers, 1)
        return headers

    existing = {h.strip() for h in headers if h}
    updated = False

    for col in ["apply_url", "remote_scope"]:
        if col not in existing:
            headers.append(col)
            sheet.update_cell(1, len(headers), col)
            updated = True

    if updated:
        headers = sheet.row_values(1)

    return headers


def _find_nodesk_job_links() -> List[str]:
    """
    Scrape https://nodesk.co/remote-jobs/ and return a list of full URLs
    for individual job pages.

    We explicitly ignore:
    - /remote-jobs/collections/
    - /remote-jobs/remote-first/<category>/
    - /remote-jobs/tags/<tag>/
    """
    logger.info("Fetching NoDesk jobs index: %s", NODESK_JOBS_URL)
    resp = requests.get(NODESK_JOBS_URL, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    links: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("/remote-jobs/"):
            continue

        # Normalize to absolute
        if href == "/remote-jobs/":
            continue

        if any(skip in href for skip in ["/collections", "/remote-first/", "/tags/"]):
            continue

        full = NODESK_BASE_URL + href if href.startswith("/") else href
        links.add(full)

    logger.info("Found %d potential NoDesk job links", len(links))
    return sorted(links)


def _parse_nodesk_job(url: str, headers: List[str]) -> Optional[Dict[str, Any]]:
    """
    Parse a single NoDesk job page into a row dict.
    """
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Failed to fetch NoDesk job page %s: %s", url, e)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Title
    h1 = soup.find("h1")
    title = _normalize_text(h1.get_text()) if h1 else ""

    if not title:
        logger.debug("Skipping NoDesk job with empty title at %s", url)
        return None

    # Company – often the first <p> under the title or a strong tag
    company = ""
    if h1:
        parent = h1.parent
        if parent:
            # look for something that looks like company name near the title
            p = parent.find("p")
            if p:
                company = _normalize_text(p.get_text())
    if not company:
        # fallback: meta tag
        meta_company = soup.find("meta", attrs={"itemprop": "hiringOrganization"})
        if meta_company and meta_company.get("content"):
            company = _normalize_text(meta_company["content"])

    # Location line – something starting with "Remote"
    location = ""
    for text_node in soup.stripped_strings:
        txt = text_node.strip()
        if txt.lower().startswith("remote"):
            location = txt
            break

    # Normalise location
    if location:
        location = location.replace("\n", " ").replace("  ", " ")
        location = location.replace("Remote:", "Remote -").replace("Remote –", "Remote -")
        location = location.replace("•", "·")

    remote_scope = compute_remote_scope(location)

    # Only keep clearly remote roles
    if remote_scope not in {"global", "regional", "country"}:
        logger.debug("Skipping NoDesk job %s due to remote_scope=%s", url, remote_scope)
        return None

    # Tags – chips / badges on the page
    tag_texts: List[str] = []
    for badge in soup.find_all(["a", "span"], class_=lambda c: c and "tag" in c.lower()):
        t = _normalize_text(badge.get_text())
        if t and t not in tag_texts:
            tag_texts.append(t)
    tags_str = ", ".join(tag_texts)

    # We don't get structured salary or posted_at from NoDesk easily -> leave blank
    min_salary_num = None
    max_salary_num = None
    currency = "USD"

    high_salary_flag = is_high_salary(min_salary_num, max_salary_num, currency)

    ingested_at = datetime.now(timezone.utc).isoformat()
    posted_at = ""  # NoDesk doesn't expose a clear posted date on the card

    # Source ID: use slug portion of URL
    slug = url.rstrip("/").split("/")[-1]
    source_job_id = slug
    row_id = f"nodesk_{source_job_id}"

    role = normalize_role(title, tag_texts)
    category = normalize_category(title, tag_texts, role)
    seniority = extract_seniority(title, tag_texts)
    employment_type = extract_employment_type(title, tag_texts)

    row_dict: Dict[str, Any] = {
        "id": row_id,
        "title": title,
        "company": company,
        "source": "NoDesk",
        "url": url,
        "source_job_id": source_job_id,
        "location": location or "Remote",
        "job_roles": role,
        "job_category": category,
        "seniority": seniority,
        "employment_type": employment_type,
        "tags": tags_str,
        "tech_stack": "",
        "min_salary": "" if min_salary_num is None else min_salary_num,
        "max_salary": "" if max_salary_num is None else max_salary_num,
        "currency": currency,
        "high_salary": "TRUE" if high_salary_flag else "FALSE",
        "posted_at": posted_at,
        "ingested_at": ingested_at,
        "apply_url": url,
        "remote_scope": remote_scope,
    }

    # Respect sheet schema: drop any keys that aren't headers
    for key in list(row_dict.keys()):
        if key not in headers:
            row_dict.pop(key, None)

    return row_dict


# --------------------------------------------------------------------
# Main ingestion pipeline
# --------------------------------------------------------------------


def ingest_nodesk() -> int:
    """
    Fetch NoDesk jobs and append new ones to the Jobs sheet.
    Returns the number of rows inserted.
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

    job_links = _find_nodesk_job_links()
    logger.info("Processing %d NoDesk job pages", len(job_links))

    new_rows: List[List[Any]] = []
    inserted = 0

    for url in job_links:
        slug = url.rstrip("/").split("/")[-1]
        key = f"NoDesk:{slug}"
        if key in existing_keys:
            continue

        row_dict = _parse_nodesk_job(url, headers)
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
