import os
import json
import gspread
from google.oauth2.service_account import Credentials

# 👉 Correct mapping:
# Jobs sheet:        1tdFJX2Wk5VRyxg2DEn_ChbnwuUAScguQXyXM7yWY7_g
# Subscribers sheet: 1viyKdvfA5BN3g1gF8ZMWlTZmsafiXwOqNw6oVgkguJ0

JOBS_SHEET_ID = "1tdFJX2Wk5VRyxg2DEn_ChbnwuUAScguQXyXM7yWY7_g"
SUBSCRIBERS_SHEET_ID = "1viyKdvfA5BN3g1gF8ZMWlTZmsafiXwOqNw6oVgkguJ0"


def get_gspread_client():
    """
    Loads your Google service account from the Vercel environment variable.

    Expects GOOGLE_SERVICE_ACCOUNT_JSON to contain the *full* service account JSON.
    """
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise Exception("Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable")

    service_account_info = json.loads(sa_json)

    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(credentials)


def get_jobs_sheet():
    """
    Return the Jobs worksheet (first tab) for reading/writing jobs.

    Schema (columns, in any order, but typically):
      id, title, company, source, url, apply_url, source_job_id, location,
      job_roles, job_category, seniority, employment_type,
      tags, tech_stack, min_salary, max_salary, currency,
      high_salary, posted_at, ingested_at, remote_scope, ...
    """
    gc = get_gspread_client()
    sh = gc.open_by_key(JOBS_SHEET_ID)
    # ✅ ALWAYS use the *first* worksheet.
    # This works no matter what the tab is named ("Sheet1", "Jobs_sheet1", etc.)
    return sh.sheet1


def get_subscribers_sheet():
    """
    Return the Subscribers worksheet (first tab) for reading/writing subscribers.

    Expected columns include (can have more):
      email, first_name, job_roles, job_category, experience_level,
      location_pref, employment_type, high_salary_only, technologies_pref,
      languages_pref, company_pref, search_term, frequency,
      last_sent_at, sent_job_ids, ...
    """
    gc = get_gspread_client()
    sh = gc.open_by_key(SUBSCRIBERS_SHEET_ID)
    return sh.sheet1  # first tab for subscribers too


def get_logs_sheet():
    """
    Return (or create) a 'Logs' worksheet inside the Jobs spreadsheet.

    Columns:
      timestamp, source, event, inserted, error, meta_json
    """
    gc = get_gspread_client()
    sh = gc.open_by_key(JOBS_SHEET_ID)

    try:
        ws = sh.worksheet("Logs")
    except Exception:
        # Create worksheet if it doesn't exist yet
        ws = sh.add_worksheet(title="Logs", rows=1000, cols=10)
        ws.append_row(
            ["timestamp", "source", "event", "inserted", "error", "meta_json"],
            value_input_option="RAW",
        )

    return ws
