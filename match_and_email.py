# match_and_email.py
"""
Match jobs to subscribers based on preferences and build personalized HTML digests.

Current behaviour:
- Reads recent jobs from Jobs sheet (last DAYS_BACK days)
- Reads Subscribers sheet
- For each subscriber:
    - Checks if it's time to send (based on frequency + last_sent_at)
    - Filters jobs by role, location, experience, salary, etc.
    - Scores and picks top N
    - Builds an HTML digest
    - DRY-RUN: prints a message instead of sending real email
    - Updates last_sent_at in the Subscribers sheet

Once you’re happy with the matches, you can plug in a real Beehiiv (or other ESP)
send function inside send_email_via_beehiiv().
"""

import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

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


def parse_iso(dt_str: str):
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
    if not value:
        return False
    return str(value).strip().lower() in ["true", "1", "yes", "y"]


def days_since(dt_str: str) -> float:
    dt = parse_iso(dt_str)
    if not dt:
        return 9999
    return (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0


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


def is_remote_or_hybrid(location: str) -> bool:
    """
    Global filter: we only want remote or hybrid jobs.

    - Keep anything whose location contains "remote" or "hybrid"
    - Filter out purely on-site locations like "New York", "Amsterdam", etc.
    """
    if not location:
        return False

    loc = str(location).lower().strip()

    if "remote" in loc:
        return True
    if "hybrid" in loc:
        return True

    return False


def match_location(job_loc: str, pref: str) -> bool:
    """
    Per-subscriber location filter (runs *after* we already enforced remote/hybrid).

    If subscriber has no preference -> accept.
    Otherwise:
      - allow "worldwide"/"anywhere"
      - simple substring match of pref inside job_loc
    """
    if not pref:
        return True
    job_loc = (job_loc or "").lower()
    pref = pref.lower()

    if "worldwide" in job_loc or "anywhere" in job_loc:
        return True

    return pref in job_loc


def match_roles(job_roles: str, pref_roles: str) -> bool:
    if not pref_roles:
        return True
    job_set = set(parse_csv(job_roles))
    pref_set = set(parse_csv(pref_roles))
    if not job_set:
        # If job has no roles tagged, you might later fall back to searching in title
        return False
    return len(job_set & pref_set) > 0


def match_experience(job_seniority: str, pref_exp: str, job_title: str) -> bool:
    if not pref_exp:
        return True

    pref_exp = pref_exp.lower()
    job_seniority = (job_seniority or "").lower()
    title = (job_title or "").lower()

    if job_seniority:
        return pref_exp in job_seniority

    keywords = {
        "junior": ["junior", "jr "],
        "mid": ["mid", "intermediate"],
        "senior": ["senior", "sr ", "sr.", "lead"],
        "lead": ["lead", "principal", "staff"],
    }
    for level, words in keywords.items():
        if any(w in title for w in words):
            return pref_exp == level or (pref_exp == "senior" and level in ["senior", "lead"])

    return True  # if unclear, don't block


def match_employment(job_type: str, pref_type: str) -> bool:
    if not pref_type:
        return True
    if not job_type:
        return True
    return pref_type.lower() in job_type.lower()


def match_high_salary(job: Dict[str, Any], high_salary_only: bool) -> bool:
    """
    If high_salary_only is True:
    - honour the boolean high_salary flag from the sheet
    - otherwise require min_salary >= 100,000 (USD or equivalent)
    """
    if not high_salary_only:
        return True

    hs = bool_from_str(job.get("high_salary"))
    if hs:
        return True

    try:
        min_salary = float(job.get("min_salary") or 0)
    except Exception:
        min_salary = 0

    # Align with UI copy: "$100k+"
    return min_salary >= 100000


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
    if match_location(job.get("location", ""), sub.get("location_pref", "")):
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

    return score


# ---------- HTML digest ----------

def build_html_digest(sub: Dict[str, Any], jobs: List[Dict[str, Any]]) -> str:
    fname = sub.get("first_name") or "there"
    job_roles = sub.get("job_roles") or "remote roles"

    intro = f"""
    <p>Hey {fname},</p>
    <p>Here are your latest <strong>{job_roles}</strong> that match your filters on ASAP Jobs 🚀</p>
    """

    if not jobs:
        intro += "<p>No perfect matches today, but we’re scanning more boards every few hours. You’ll hear from us again soon.</p>"
        return f"<html><body>{intro}</body></html>"

    items_html = ""
    for job in jobs:
        title = job.get("title") or "Untitled role"
        company = job.get("company") or ""
        # Prefer external/company apply URL if present
        url = job.get("apply_url") or job.get("url") or "#"
        location = job.get("location") or "Remote"

        salary = ""
        if job.get("min_salary") or job.get("max_salary"):
            min_sal = job.get("min_salary") or ""
            max_sal = job.get("max_salary") or ""
            cur = job.get("currency") or ""
            if min_sal and max_sal:
                salary = f"{min_sal}–{max_sal} {cur}"
            else:
                salary = f"{min_sal or max_sal} {cur}"

        tags = job.get("tags") or ""
        source = job.get("source") or ""

        items_html += f"""
        <tr>
          <td style="padding:12px 0;border-bottom:1px solid #e5e7eb;">
            <a href="{url}" style="font-size:16px;font-weight:600;color:#2563eb;text-decoration:none;">{title}</a>
            <div style="font-size:14px;color:#4b5563;margin-top:2px;">
              {company} • {location}{' • ' + salary if salary else ''}
            </div>
            <div style="font-size:12px;color:#6b7280;margin-top:4px;">
              {tags}  &nbsp;&nbsp; <span style="color:#9ca3af;">Source: {source}</span>
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
    jobs_sheet = get_jobs_sheet()
    subs_sheet = get_subscribers_sheet()

    # Read all rows as list[dict]
    jobs = jobs_sheet.get_all_records()
    subs = subs_sheet.get_all_records()

    # For updating last_sent_at we need column index
    header_row = subs_sheet.row_values(1)
    header_to_col = {name: idx + 1 for idx, name in enumerate(header_row)}
    last_sent_col = header_to_col.get("last_sent_at")

    # Only consider recent AND remote/hybrid jobs
    recent_jobs = [
        j for j in jobs
        if job_is_recent(j) and is_remote_or_hybrid(j.get("location", ""))
    ]
    print(f"Recent remote/hybrid jobs in window: {len(recent_jobs)}")

    for row_idx, sub in enumerate(subs, start=2):  # start=2 because row 1 is header
        email = sub.get("email")
        if not email:
            continue

        if not is_due_to_send(sub):
            continue

        matched = []
        high_salary_only = bool_from_str(sub.get("high_salary_only"))

        for job in recent_jobs:
            if not match_location(job.get("location", ""), sub.get("location_pref", "")):
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

        if not matched:
            print(f"No matches for {email}")
            continue

        matched.sort(key=lambda x: x[0], reverse=True)
        top_jobs = [j for _, j in matched[:TOP_N_JOBS]]

        html = build_html_digest(sub, top_jobs)
        subject = "Your ASAP Jobs remote opportunities"

        send_email_via_beehiiv(email, subject, html)

        # Update last_sent_at if column exists
        if last_sent_col:
            subs_sheet.update_cell(row_idx, last_sent_col, iso_now())

    print("Done matching & (dry-run) sending.")


if __name__ == "__main__":
    main()
