# logging_utils.py
"""
Lightweight logging helper for ingestion and matching.

Right now this just prints a structured line so you can see it
in Vercel logs. Later you can extend it to write into a Logs sheet.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger("asap_jobs_logging")
logger.setLevel(logging.INFO)


def log_event(source: str, event: str, **kwargs: Any) -> None:
    """
    Log a structured event. Safe no-op if anything goes wrong.

    Example:
        log_event("RemoteOK", "ingest_ok", inserted=42)
    """
    try:
        payload: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "event": event,
        }
        if kwargs:
            payload.update(kwargs)

        logger.info("[EVENT] %s", json.dumps(payload, ensure_ascii=False))
        print("[EVENT]", json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        # Never break ingestion because of logging
        logger.warning("log_event failed: %s", e)
