# sheets_client.py
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# Your Google Sheet IDs (already filled in)
JOBS_SHEET_ID = "1viyKdvfA5BN3g1gF8ZMWlTZmsafiXwOqNw6oVgkguJ0"
SUBSCRIBERS_SHEET_ID = "1tdFJX2Wk5VRyxg2DEn_ChbnwuUAScguQXyXM7yWY7_g"

def get_gspread_client():
    """
    Loads your Google service account from the Vercel environment variable.
    """
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise Exception("Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable")

    service_account_info = json.loads(sa_json)

    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(credentials)

def get_jobs_sheet():
    gc = get_gspread_client()
    sh = gc.open_by_key(JOBS_SHEET_ID)
    return sh.sheet1  # uses first tab

def get_subscribers_sheet():
    gc = get_gspread_client()
    sh = gc.open_by_key(SUBSCRIBERS_SHEET_ID)
    return sh.sheet1  # uses first tab
