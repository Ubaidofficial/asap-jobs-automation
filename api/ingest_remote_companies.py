# api/ingest_remote_companies.py

from http.server import BaseHTTPRequestHandler
import json
import logging

from remote_companies_ingest import ingest_remote_companies

logger = logging.getLogger("api.ingest_remote_companies")
logger.setLevel(logging.INFO)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Vercel Python entrypoint for GET /api/ingest_remote_companies

        - Calls ingest_remote_companies()
        - Only inserts Remote + Hybrid jobs (via remote_scope filter)
        - Returns JSON summary
        """
        try:
            inserted = ingest_remote_companies()
            status = 200
            payload = {
                "status": "success",
                "source": "remote_companies",
                "inserted": inserted,
                "message": f"Ingested {inserted} new jobs from remote-first companies",
            }
        except Exception as e:
            logger.exception("Error in /api/ingest_remote_companies: %s", e)
            status = 500
            payload = {
                "status": "error",
                "source": "remote_companies",
                "inserted": 0,
                "message": str(e),
            }

        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
