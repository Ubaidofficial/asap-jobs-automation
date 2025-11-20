# remote_companies_ingest.py
"""
Placeholder ingest for remote-first companies' career pages.

Right now this:

- Ensures the Jobs sheet headers exist
- Loads existing records (using expected_headers to avoid gspread header error)
- Returns 0 inserted (no actual scraping yet)

You can extend this later to actually scrape companies' career pages and
insert rows using the same schema as RemoteOK / Remotive.
"""

import logging
from typing import Set

from sheets_client import get_jobs_sheet
from remoteok_ingest import _ensure_headers, _normalize_text

logger = logging.getLogger("remote_companies_ingest")
logger.setLevel(logging.INFO)


def ingest_remote_companies() -> int:
    """
    Placeholder implementation: does not yet insert jobs, but is
    robust against header issues so /api/ingest_all works cleanly.
    """
    sheet = get_jobs_sheet()
    headers = _ensure_headers(sheet)

    # Use expected_headers to avoid "header row not unique" error
    existing_records = sheet.get_all_records(expected_headers=headers)

    existing_keys: Set[str] = set()
    for row in existing_records:
        source = _normalize_text(row.get("source"))
        sid = _normalize_text(str(row.get("source_job_id", "")))
        if source and sid:
            existing_keys.add(f"{source}:{sid}")

    logger.info(
        "Loaded %d existing rows from Jobs sheet (remote companies placeholder)",
        len(existing_records),
    )

    # TODO: implement real scraping here.
    logger.info("Remote companies ingest is currently a no-op (0 rows inserted).")
    return 0


if __name__ == "__main__":
    count = ingest_remote_companies()
    print(f"Ingested {count} jobs from remote companies (placeholder): {count}")
