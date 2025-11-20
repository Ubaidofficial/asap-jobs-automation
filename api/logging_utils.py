# logging_utils.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sheets_client import get_logs_sheet


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_event(
    source: str,
    event: str,
    inserted: int = 0,
    error: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Append a simple event row into Logs sheet.

    source:  'RemoteOK', 'Remotive', 'RemoteCompanies', 'WeWorkRemotely', 'Match'
    event:   'ingest_ok', 'ingest_error', 'match_ok', 'match_error', etc.
    inserted: number of rows inserted / emails sent
    error:   error message (if any)
    meta:    extra dictionary, will be stored as JSON
    """
    ws = get_logs_sheet()

    meta_json = ""
    if meta:
        try:
            meta_json = json.dumps(meta, ensure_ascii=False)
        except Exception:
            meta_json = str(meta)

    row = [
        _now_iso(),
        source,
        event,
        inserted,
        error or "",
        meta_json,
    ]
    ws.append_row(row, value_input_option="RAW")
