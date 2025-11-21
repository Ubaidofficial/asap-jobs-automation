# api/nodesk_ingest.py

from http.server import BaseHTTPRequestHandler
import json
import logging

from nodesk_ingest import ingest_nodesk

logger = logging.getLogger("api.nodesk_ingest")
logger.setLevel(logging.INFO)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Vercel Python entrypoint for GET /api/nodesk_ingest

        - Calls ingest_nodesk()
        - Returns JSON summary with how many jobs were ingested
        """
        try:
            inserted = ingest_nodesk()
            status = 200
            payload = {
                "status": "success",
                "source": "nodesk",
                "inserted": inserted,
                "message": f"Ingested {inserted} new NoDesk jobs",
            }
        except Exception as e:
            logger.exception("Error in /api/nodesk_ingest: %s", e)
            status = 500
            payload = {
                "status": "error",
                "source": "nodesk",
                "inserted": 0,
                "message": str(e),
            }

        body = json.dumps(payload).encode("utf-8")

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
