# api/ingest_remotive.py

import json
import traceback
from http.server import BaseHTTPRequestHandler

from remotive_ingest import ingest_remotive


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Vercel Python entrypoint for GET /api/ingest_remotive

        - Calls ingest_remotive()
        - Returns JSON with how many jobs were ingested
        """

        try:
            inserted = ingest_remotive()
            status = 200
            payload = {
                "status": "success",
                "source": "remotive",
                "inserted": inserted,
                "message": f"Ingested {inserted} new Remotive jobs",
            }
        except Exception as e:
            # Full traceback into Vercel logs
            traceback.print_exc()

            status = 500
            payload = {
                "status": "error",
                "source": "remotive",
                "message": str(e),
            }

        body = json.dumps(payload).encode("utf-8")

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
