# api/ingest_remoteok.py

from http.server import BaseHTTPRequestHandler
import json
import logging

from remoteok_ingest import ingest_remoteok

logger = logging.getLogger("api.ingest_remoteok")
logger.setLevel(logging.INFO)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Vercel Python entrypoint for GET /api/ingest_remoteok

        - Calls ingest_remoteok()
        - Returns JSON summary with how many jobs were ingested
        """

        try:
            inserted = ingest_remoteok()
            status = 200
            payload = {
                "status": "success",
                "source": "remoteok",
                "inserted": inserted,
                "message": f"Ingested {inserted} new RemoteOK jobs",
            }
        except Exception as e:
            logger.exception("Error in /api/ingest_remoteok: %s", e)
            status = 500
            payload = {
                "status": "error",
                "source": "remoteok",
                "inserted": 0,
                "message": str(e),
            }

        body = json.dumps(payload)

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
