# api/weworkremotely_ingest.py

from http.server import BaseHTTPRequestHandler
import json
import logging

from weworkremotely_ingest import ingest_weworkremotely

logger = logging.getLogger("api.weworkremotely_ingest")
logger.setLevel(logging.INFO)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Vercel Python entrypoint for GET /api/weworkremotely_ingest

        - Calls ingest_weworkremotely()
        - Only inserts Remote / Hybrid jobs (via remote_scope filter)
        - Returns JSON summary
        """
        try:
            inserted = ingest_weworkremotely()
            status = 200
            payload = {
                "status": "success",
                "source": "weworkremotely",
                "inserted": inserted,
                "message": f"Ingested {inserted} new WeWorkRemotely jobs",
            }
        except Exception as e:
            logger.exception("Error in /api/weworkremotely_ingest: %s", e)
            status = 500
            payload = {
                "status": "error",
                "source": "weworkremotely",
                "inserted": 0,
                "message": str(e),
            }

        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
