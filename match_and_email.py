# match_and_email.py
"""
Match jobs to subscribers based on preferences and build personalized HTML digests.

Current behaviour (enhanced):
- Reads recent jobs from Jobs sheet (last DAYS_BACK days)
- Reads Subscribers sheet
- For each subscriber:
    - Checks if it's time to send (based on frequency + last_sent_at)
    - Filters jobs by:
        - remote_scope (remote/hybrid only)
        - location (using remote_scope + location_pref)
        - role (with canonical role expansion)
        - experience/seniority
        - employment type
        - salary (including high_salary flag)
        - tech/language prefs
        - company prefs
        - free-text search term
    - Dedupes jobs across sources using a fingerprint
    - Scores and picks top N
    - Builds an HTML digest (uses apply_url as primary link)
    - DRY-RUN: prints a message instead of sending real email
    - Updates last_sent_at in the Subscribers sheet

Once you’re happy with the matches, you can plug in a real Beehiiv (or other ESP)
send function inside send_email_via_beehiiv().
"""

import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Set, Tuple

from dotenv import load_dotenv

from sheets_client import get_jobs_sheet, get_subscribers_sheet

load_dotenv()

# ---------- Config ----------

# Jobs window: consider jobs from the last N days
DAYS_BACK = int(os.environ.get("MATCH_DAYS_BACK", "2"))

# Maximum number of jobs per subscriber digest
TOP_N_JOBS = int(os.environ.get("TOP_N_JOBS", "10"))


# ---------- General helpers ----------

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_csv(value: str) -> List[str]:
    if not value:
        return []
    return [x.strip().lower() for x in value.split(",") if x.strip()]


def bool_from_str(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def days_since(dt_str: str) -> float:
    dt = parse_iso(dt_str)
    if not dt:
        return 9999
    return (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0


def normalize_text(s: Any) -> str:
    return (str(s) if s is not None else "").strip().lower()


def parse_salary(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


# ---------- Frequency logic ----------

def _normalize_freq(raw: str) -> str:
    """
    Normalize various frequency strings from the sheet / UI into:
    - "daily"
    - "twice_weekly"
    - "weekly"
    """
    if not raw:
        return "daily"
    f = raw.strip().lower()

    # UI sends "2x" for 2× per week
    if f in {"2x", "2x per week", "2x/week", "twice_weekly", "2x_week", "twice weekly"}:
        return "twice_weekly"
    if f == "weekly":
        return "weekly"
    if f == "daily":
        return "daily"

    # Fallback to daily if unknown
    return "daily"


def is_due_to_send(sub: Dict[str, Any]) -> bool:
    """
    Decide if we should send a digest to this subscriber today,
    based on frequency + last_sent_at.
    """
    freq = _normalize_freq(sub.get("frequency") or "")
    last_sent = sub.get("last_sent_at") or ""
    days = days_since(last_sent)

    if freq == "daily":
        return days >= 1
    if freq == "twice_weekly":
        # ~2x per week → every 3 days
        return days >= 3
    if freq == "weekly":
        return days >= 7

    # fallback (shouldn't hit because of _normalize_freq)
    return days >= 1


# ---------- Canonical roles / expansion ----------

# Canonical roles we expect from ingestion (normalize_role) and for subscribers
CANONICAL_ROLE_GROUPS: Dict[str, Set[str]] = {
    # Engineering
    "software engineer": {
        "software engineer", "software developer", "swe",
        "backend engineer", "frontend engineer", "full-stack engineer",
        "full stack engineer", "mobile engineer",
        "devops engineer", "site reliability engineer", "sre",
        "platform engineer",
    },
    "backend engineer": {
        "backend engineer", "backend developer", "server engineer",
    },
    "frontend engineer": {
        "frontend engineer", "front-end engineer", "front end engineer", "ui engineer",
    },
    "full-stack engineer": {
        "full-stack engineer", "full stack engineer", "fullstack engineer",
    },
    "devops engineer": {
        "devops engineer", "site reliability engineer", "sre", "platform engineer",
    },
    "data engineer": {
        "data engineer", "analytics engineer", "data platform engineer",
    },
    "data scientist": {
        "data scientist", "ml engineer", "machine learning engineer",
    },
    "data analyst": {
        "data analyst", "business intelligence analyst", "bi analyst",
    },

    # Product & Design
    "product manager": {
        "product manager", "product owner", "product lead",
    },
    "product designer": {
        "product designer", "ux/ui designer", "ux designer", "ui designer",
    },

    # Marketing / Growth
    "marketing manager": {
        "marketing manager", "digital marketing manager", "product marketing manager",
    },
    "growth marketer": {
        "growth marketer", "performance marketer", "paid media manager",
    },
    "content marketer": {
        "content marketer", "copywriter", "copy writer", "content writer",
    },

    # Sales / CS
    "sales representative": {
        "sales representative", "sales dev rep", "sdr", "bdr", "inside sales",
    },
    "account executive": {
        "account executive", "ae",
    },
    "customer success manager": {
        "customer success manager", "customer success", "cs manager",
    },
    "support specialist": {
        "support specialist", "customer support", "technical support", "helpdesk",
    },

    # People / Ops
    "recruiter": {
        "recruiter", "talent acquisition", "talent partner",
    },
    "hr generalist": {
        "hr generalist", "hr specialist",
    },
    "people operations": {
        "people operations", "people ops", "people operations manager",
    },
    "project manager": {
        "project manager", "program manager", "delivery manager",
    },
    "operations manager": {
        "operations manager", "business operations", "ops manager",
    },

    # Finance / Legal
    "finance manager": {
        "finance manager", "fp&a manager", "financial analyst",
    },
    "accountant": {
        "accountant", "senior accountant",
    },
    "legal counsel": {
        "legal counsel", "corporate counsel", "attorney", "lawyer",
    },

    # Leadership
    "founder / ceo": {"founder", "co-founder", "ceo"},
    "cto": {"cto", "chief technology officer"},
    "cpo": {"cpo", "chief product officer"},
    "coo": {"coo", "chief operating officer"},

    # Catch-all
    "other": {"other"},
}


def expand_subscriber_roles(raw_roles: str) -> Set[str]:
    """
    Subscriber may choose a few roles (comma-separated, free text).
    We expand them to a set of canonical roles we can match against.
    """
    if not raw_roles:
        return set()

    tokens = parse_csv(raw_roles)
    expanded: Set[str] = set()

    # Map subscriber text into canonical bucket(s)
    for token in tokens:
        matched_any = False
        for canonical, group in CANONICAL_ROLE_GROUPS.items():
            # if token literally equals a canonical or is in the synonyms
            if token == canonical or token in group:
                expanded.add(canonical)
                matched_any = True
        # if we didn't match anything, keep the raw token as a 'canonical-ish' key
        if not matched_any:
            expanded.add(token)

    return expanded


def job_role_to_canonical(job_role: str) -> str:
    """
    Normalize job_roles text from sheet into a simple canonical key for matching.
    """
    if not job_role:
        return ""
    jr = job_role.strip().lower()
    for canonical, group in CANONICAL_ROLE_GROUPS.items():
        if jr == canonical or jr in group:
            return canonical
    return jr


# ---------- Matching helpers ----------

def job_is_recent(job: Dict[str, Any], days_back: int = DAYS_BACK) -> bool:
    posted = parse_iso(job.get("posted_at") or "")
    ingested = parse_iso(job.get("ingested_at") or "")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    if posted and posted >= cutoff:
        return True
    if ingested and ingested >= cutoff:
        return True
    return False


def compute_job_fingerprint(job: Dict[str, Any]) -> str:
    """
    Cross-source dedupe key.
    Example: "software engineer::acme inc::remote - europe"
    """
    title = normalize_text(job.get("title"))
    company = normalize_text(job.get("company"))
    location = normalize_text(job.get("location"))

    # some boards include "remote" in the title – we strip generic 'remote' tokens
    for bad in ("remote -", "remote/", "(remote)", "[remote]", "remote job", "remote"):
        title = title.replace(bad, "").strip()

    return f"{title}::{company}::{location}"


def match_remote_scope(job: Dict[str, Any]) -> bool:
    """
    Only allow clearly remote/hybrid-like jobs.

    We assume remote_scope ∈ {global, country, regional, onsite, unknown, mixed, ""}.
    For now we accept:
        - global      (worldwide)
        - country     (e.g. "USA", "Canada", "UK")
        - regional    (e.g. "EMEA", "LATAM", "CET +/- 3")
    We reject:
        - onsite
        - anything empty/unknown (to stay strict for now)
    """
    scope = (job.get("remote_scope") or "").strip().lower()
    if not scope:
        return False

    return scope in {"global", "country", "regional"}


def _location_pref_tokens(pref: str) -> Set[str]:
    if not pref:
        return set()
    tokens = {t.strip().lower() for t in pref.replace("/", ",").split(",") if t.strip()}
    # normalize a few common cases
    norm_map = {
        "us": "usa",
        "united states": "usa",
        "united states of america": "usa",
        "uk": "united kingdom",
        "england": "united kingdom",
        "scotland": "united kingdom",
        "wales": "united kingdom",
        "europe": "europe",
        "latam": "latam",
        "mea": "mea",
        "apac": "apac",
        "anywhere": "anywhere",
        "worldwide": "anywhere",
        "remote": "anywhere",
    }
    normalized: Set[str] = set()
    for t in tokens:
        normalized.add(norm_map.get(t, t))
    return normalized


def match_location(job: Dict[str, Any], location_pref: str) -> bool:
    """
    Smarter location matching that uses both `location` and `remote_scope`.

    Rules:
    - If no subscriber preference: rely only on match_remote_scope() (remote-only).
    - If subscriber pref is "anywhere/remote/worldwide": any accepted remote_scope is OK.
    - If job scope is 'global': always OK (subject to remote_scope).
    - If job scope is 'country': we try to see if country name or code appears in the pref.
    - If job scope is 'regional': we check for region tokens like 'europe', 'latam', 'emea'.
    """
    if not match_remote_scope(job):
        return False

    pref_tokens = _location_pref_tokens(location_pref)
    job_loc = normalize_text(job.get("location") or "")
    scope = (job.get("remote_scope") or "").strip().lower()

    # no preference but remote_scope is already good
    if not pref_tokens:
        return True

    # "anywhere" / "remote" / "worldwide"
    if "anywhere" in pref_tokens:
        return True

    # global jobs can match any preference
    if scope == "global":
        return True

    # simple region heuristics
    region_keywords = {
        "europe": ["europe", "emea", "eu"],
        "latam": ["latam", "latin america"],
        "apac": ["apac", "asia pacific"],
        "africa": ["africa"],
    }

    # If subscriber specifies regions, check them
    for region, kws in region_keywords.items():
        if region in pref_tokens:
            if any(k in job_loc for k in kws):
                return True

    # Country-level matching: if pref tokens appear in location text
    for token in pref_tokens:
        if token and token in job_loc:
            return True

    # Fallback: loose substring matching
    for token in pref_tokens:
        if token and token in job_loc:
            return True

    return False


def match_roles(job_roles: str, sub_roles_raw: str) -> bool:
    """
    Compare subscriber's expanded canonical roles against the job's canonical role.
    """
    if not sub_roles_raw:
        return True

    expanded_sub_roles = expand_subscriber_roles(sub_roles_raw)
    job_canon = job_role_to_canonical(job_roles)

    if not job_canon:
        # if job has no role at all, be conservative and reject
        return False

    # direct match
    if job_canon in expanded_sub_roles:
        return True

    # also allow synonym match across groups
    for canonical, group in CANONICAL_ROLE_GROUPS.items():
        if job_canon in group and canonical in expanded_sub_roles:
            return True

    return False


def match_experience(job_seniority: str, pref_exp: str, job_title: str) -> bool:
    if not pref_exp:
        return True

    pref_exp = pref_exp.strip().lower()
    job_seniority = (job_seniority or "").strip().lower()
    title = (job_title or "").strip().lower()

    # direct match on canonical labels
    if job_seniority:
        return pref_exp in job_seniority

    # heuristic fallback based on title
    keywords = {
        "junior": ["junior", "jr "],
        "mid": ["mid", "intermediate"],
        "senior": ["senior", "sr ", "sr.", "lead"],
        "lead": ["lead", "principal", "staff"],
        "director": ["director", "head of"],
        "vp": ["vp ", "vice president"],
    }
    for level, words in keywords.items():
        if any(w in title for w in words):
            if pref_exp == level:
                return True
            # allow "senior" pref to also catch lead/principal/staff
            if pref_exp == "senior" and level in {"senior", "lead"}:
                return True
            # allow "lead" pref to also catch director
            if pref_exp == "lead" and level in {"lead", "director"}:
                return True
            # allow "vp" to match VP/C-level
            if pref_exp == "vp" and level == "vp":
                return True

    # if unclear, don't block
    return True


def match_employment(job_type: str, pref_type: str) -> bool:
    if not pref_type:
        return True
    if not job_type:
        return True
    return pref_type.strip().lower() in job_type.strip().lower()


def match_high_salary(job: Dict[str, Any], high_salary_only: bool) -> bool:
    """
    If high_salary_only is True:
    - honour the boolean high_salary flag from the sheet
    - otherwise require min_salary or max_salary >= 100,000 (USD-ish)
    """
    if not high_salary_only:
        return True

    hs = bool_from_str(job.get("high_salary"))
    if hs:
        return True

    min_salary = parse_salary(job.get("min_salary"))
    max_salary = parse_salary(job.get("max_salary"))
    candidate = max(filter(lambda x: x is not None, [min_salary, max_salary]), default=None)

    if candidate is None:
        return False

    # Align with UI copy: "$100k+"
    return candidate >= 100000.0


def match_tech_and_lang(job: Dict[str, Any], sub: Dict[str, Any]) -> bool:
    tech_pref = parse_csv(sub.get("technologies_pref") or "")
    lang_pref = parse_csv(sub.get("languages_pref") or "")

    if not tech_pref and not lang_pref:
        return True

    job_tech = set(parse_csv(job.get("tech_stack") or ""))
    job_tags = set(parse_csv(job.get("tags") or ""))

    if tech_pref and job_tech and job_tech & set(tech_pref):
        return True
    if lang_pref and job_tags and job_tags & set(lang_pref):
        return True

    if tech_pref or lang_pref:
        return False
    return True


def match_company(job_company: str, company_pref: str) -> bool:
    if not company_pref:
        return True
    prefs = parse_csv(company_pref)
    job_company = (job_company or "").lower()
    return any(p in job_company for p in prefs)


def match_search_term(job: Dict[str, Any], term: str) -> bool:
    if not term:
        return True
    term = term.lower()
    text = " ".join([
        str(job.get("title") or ""),
        str(job.get("company") or ""),
        str(job.get("tags") or ""),
        str(job.get("job_roles") or ""),
    ]).lower()
    return term in text


def score_job(job: Dict[str, Any], sub: Dict[str, Any]) -> int:
    score = 0

    if match_roles(job.get("job_roles", ""), sub.get("job_roles", "")):
        score += 3
    if match_location(job, sub.get("location_pref", "")):
        score += 2
    if match_experience(job.get("seniority", ""), sub.get("experience_level", ""), job.get("title", "")):
        score += 2
    if match_employment(job.get("employment_type", ""), sub.get("employment_type", "")):
        score += 1
    if match_tech_and_lang(job, sub):
        score += 1
    if match_company(job.get("company", ""), sub.get("company_pref", "")):
        score += 1
    if bool_from_str(sub.get("high_salary_only")) and match_high_salary(job, True):
        score += 1

    posted = parse_iso(job.get("posted_at") or job.get("ingested_at") or "")
    if posted:
        age_days = (datetime.now(timezone.utc) - posted).total_seconds() / 86400.0
        if age_days <= 1:
            score += 2
        elif age_days <= 2:
            score += 1

    # micro bonus for "global" remote scope
    scope = (job.get("remote_scope") or "").strip().lower()
    if scope == "global":
        score += 1

    return score


def format_salary_range(job: Dict[str, Any]) -> str:
    min_sal = parse_salary(job.get("min_salary"))
    max_sal = parse_salary(job.get("max_salary"))
    cur = (job.get("currency") or "").upper()

    if min_sal is None and max_sal is None:
        return ""

    # integer or 1 decimal
    def fmt(v: float) -> str:
        if v.is_integer():
            return f"{int(v):,}"
        return f"{v:,.1f}"

    if min_sal is not None and max_sal is not None:
        return f"{fmt(min_sal)}–{fmt(max_sal)} {cur}".strip()
    if max_sal is not None:
        return f"up to {fmt(max_sal)} {cur}".strip()
    if min_sal is not None:
        return f"from {fmt(min_sal)} {cur}".strip()
    return ""


# ---------- HTML digest ----------

def build_html_digest(sub: Dict[str, Any], jobs: List[Dict[str, Any]]) -> str:
    fname = sub.get("first_name") or "there"
    job_roles_pref = sub.get("job_roles") or "remote roles"

    intro = f"""
    <p>Hey {fname},</p>
    <p>Here are your latest <strong>{job_roles_pref}</strong> that match your filters on ASAP Jobs 🚀</p>
    """

    if not jobs:
        intro += "<p>No perfect matches today, but we’re scanning more boards every few hours. You’ll hear from us again soon.</p>"
        return f"<html><body>{intro}</body></html>"

    items_html = ""
    for job in jobs:
        title = job.get("title") or "Untitled role"
        company = job.get("company") or ""
        location = job.get("location") or "Remote"
        source = job.get("source") or ""
        job_roles = job.get("job_roles") or ""
        job_category = job.get("job_category") or ""
        seniority = job.get("seniority") or ""
        employment_type = job.get("employment_type") or ""
        scope = (job.get("remote_scope") or "").title() if job.get("remote_scope") else ""

        # URLs
        apply_url = job.get("apply_url") or job.get("url") or "#"
        board_url = job.get("url") or apply_url or "#"

        salary_str = format_salary_range(job)
        tags = job.get("tags") or ""
        high_salary_flag = bool_from_str(job.get("high_salary"))

        meta_line_parts = [company, location]
        if salary_str:
            meta_line_parts.append(salary_str)
        meta_line = " • ".join([p for p in meta_line_parts if p])

        # role/category/scope chips
        chips: List[str] = []
        if job_roles:
            chips.append(job_roles)
        if job_category:
            chips.append(job_category)
        if seniority:
            chips.append(seniority)
        if employment_type:
            chips.append(employment_type)
        if scope:
            chips.append(scope)

        chips_html = ""
        if chips:
            chips_html = " ".join(
                f'<span style="display:inline-block;padding:2px 6px;margin-right:4px;margin-top:4px;font-size:11px;border-radius:999px;background:#eff6ff;color:#1d4ed8;">{c}</span>'
                for c in chips
            )

        high_salary_html = ""
        if high_salary_flag:
            high_salary_html = '<span style="display:inline-block;padding:2px 6px;margin-left:6px;font-size:11px;border-radius:999px;background:#ecfeff;color:#0f766e;">💰 High-paying</span>'

        items_html += f"""
        <tr>
          <td style="padding:12px 0;border-bottom:1px solid #e5e7eb;">
            <a href="{apply_url}" style="font-size:16px;font-weight:600;color:#2563eb;text-decoration:none;">
              {title}
            </a>
            {high_salary_html}
            <div style="font-size:14px;color:#4b5563;margin-top:2px;">
              {meta_line}
            </div>
            <div style="margin-top:4px;">
              {chips_html}
            </div>
            <div style="font-size:12px;color:#6b7280;margin-top:6px;">
              {tags}
            </div>
            <div style="font-size:11px;color:#9ca3af;margin-top:4px;">
              Source: {source} ·
              <a href="{board_url}" style="color:#6b7280;text-decoration:underline;">View on board</a>
            </div>
          </td>
        </tr>
        """

    outro = """
    <p style="margin-top:20px;font-size:12px;color:#6b7280;">
      You're receiving this because you subscribed to ASAP Jobs and set these preferences.<br>
      You can update filters or frequency any time from the link in the email footer.
    </p>
    """

    html = f"""
    <html>
      <body style="font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f9fafb;padding:24px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;margin:0 auto;background:#ffffff;padding:24px;border-radius:12px;border:1px solid #e5e7eb;">
          <tr>
            <td>
              <h1 style="font-size:20px;margin:0 0 12px 0;">Your ASAP Jobs digest</h1>
              {intro}
              <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:12px;">
                {items_html}
              </table>
              {outro}
            </td>
          </tr>
        </table>
      </body>
    </html>
    """
    return html


# ---------- Sending (currently DRY RUN) ----------

def send_email_via_beehiiv(to_email: str, subject: str, html: str):
    """
    Placeholder send function.
    Right now it just prints to console so you can test matching logic.

    Later:
    - Replace this with actual Beehiiv / transactional ESP API call.
    """
    print(f"[DRY RUN] Would send email to {to_email} with subject '{subject}' (HTML length: {len(html)})")


# ---------- Main flow ----------

def main():
    print("[MATCH] Starting ASAP Jobs matching run...")
    jobs_sheet = get_jobs_sheet()
    subs_sheet = get_subscribers_sheet()

    # Read all rows as list[dict]
    jobs = jobs_sheet.get_all_records()
    subs = subs_sheet.get_all_records()

    # For updating last_sent_at we need column index
    header_row = subs_sheet.row_values(1)
    header_to_col = {name: idx + 1 for idx, name in enumerate(header_row)}
    last_sent_col = header_to_col.get("last_sent_at")

    # Filter to recent + remote-only jobs upfront
    recent_jobs = [j for j in jobs if job_is_recent(j)]
    print(f"[MATCH] Recent jobs in window: {len(recent_jobs)}")

    remote_jobs = [j for j in recent_jobs if match_remote_scope(j)]
    print(f"[MATCH] Remote/hybrid-eligible jobs after remote_scope filter: {len(remote_jobs)}")

    for row_idx, sub in enumerate(subs, start=2):  # start=2 because row 1 is header
        email = sub.get("email")
        if not email:
            continue

        if not is_due_to_send(sub):
            continue

        print(f"[MATCH] Evaluating subscriber: {email}")
        matched: List[Tuple[int, Dict[str, Any]]] = []
        seen_fingerprints: Set[str] = set()
        high_salary_only = bool_from_str(sub.get("high_salary_only"))

        for job in remote_jobs:
            # cross-source dedupe based on fingerprint
            fp = compute_job_fingerprint(job)
            if fp in seen_fingerprints:
                continue

            # Core filters
            if not match_location(job, sub.get("location_pref", "")):
                continue
            if not match_roles(job.get("job_roles", ""), sub.get("job_roles", "")):
                continue
            if not match_experience(job.get("seniority", ""), sub.get("experience_level", ""), job.get("title", "")):
                continue
            if not match_employment(job.get("employment_type", ""), sub.get("employment_type", "")):
                continue
            if not match_high_salary(job, high_salary_only):
                continue
            if not match_tech_and_lang(job, sub):
                continue
            if not match_company(job.get("company", ""), sub.get("company_pref", "")):
                continue
            if not match_search_term(job, sub.get("search_term", "")):
                continue

            s = score_job(job, sub)
            if s <= 0:
                continue

            matched.append((s, job))
            seen_fingerprints.add(fp)

        if not matched:
            print(f"[MATCH] No matches for {email}")
            continue

        matched.sort(key=lambda x: x[0], reverse=True)
        top_jobs = [j for _, j in matched[:TOP_N_JOBS]]
        print(f"[MATCH] Subscriber {email} -> {len(top_jobs)} jobs selected")

        html = build_html_digest(sub, top_jobs)
        subject = "Your ASAP Jobs remote opportunities"

        send_email_via_beehiiv(email, subject, html)

        # Update last_sent_at if column exists
        if last_sent_col:
            subs_sheet.update_cell(row_idx, last_sent_col, iso_now())

    print("[MATCH] Done matching & (dry-run) sending.")
