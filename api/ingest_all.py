# api/ingest_all.py

from http.server import BaseHTTPRequestHandler
import json
import logging

from remoteok_ingest import ingest_remoteok
from remotive_ingest import ingest_remotive
from remote_companies_ingest import ingest_remote_companies

logger = logging.getLogger("api.ingest_all")
logger.setLevel(logging.INFO)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Vercel Python entrypoint for GET /api/ingest_all

        Calls:
        - ingest_remoteok()
        - ingest_remotive()
        - ingest_remote_companies()

        Returns JSON with per-source counts and any errors.
        """
        results = {}
        overall_ok = True

        # RemoteOK
        try:
            inserted_remoteok = ingest_remoteok()
            results["remoteok"] = {
                "status": "success",
                "inserted": inserted_remoteok,
            }
        except Exception as e:
            logger.exception("Error ingesting RemoteOK: %s", e)
            results["remoteok"] = {
                "status": "error",
                "inserted": 0,
                "error": str(e),
            }
            overall_ok = False

        # Remotive
        try:
            inserted_remotive = ingest_remotive()
            results["remotive"] = {
                "status": "success",
                "inserted": inserted_remotive,
            }
        except Exception as e:
            logger.exception("Error ingesting Remotive: %s", e)
            results["remotive"] = {
                "status": "error",
                "inserted": 0,
                "error": str(e),
            }
            overall_ok = False

        # Remote-first companies
        try:
            inserted_remote_companies = ingest_remote_companies()
            results["remote_companies"] = {
                "status": "success",
                "inserted": inserted_remote_companies,
            }
        except Exception as e:
            logger.exception("Error ingesting remote companies: %s", e)
            results["remote_companies"] = {
                "status": "error",
                "inserted": 0,
                "error": str(e),
            }
            overall_ok = False

        payload = {
            "status": "success" if overall_ok else "partial",
            "message": "Ingested from all sources (see per-source results).",
            "sources": results,
        }

        body = json.dumps(payload).encode("utf-8")

        self.send_response(200 if overall_ok else 207)  # 207 = multi-status-ish
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
