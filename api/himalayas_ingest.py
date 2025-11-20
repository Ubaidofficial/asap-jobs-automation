from http.server import BaseHTTPRequestHandler
import json
import logging

from himalayas_ingest import ingest_himalayas

logger = logging.getLogger("api.himalayas_ingest")
logger.setLevel(logging.INFO)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        GET /api/himalayas_ingest
        """
        try:
            inserted = ingest_himalayas()
            status = 200
            payload = {
                "status": "success",
                "source": "himalayas",
                "inserted": inserted,
                "message": f"Ingested {inserted} new Himalayas jobs",
            }
        except Exception as e:
            logger.exception("Error in /api/himalayas_ingest: %s", e)
            status = 500
            payload = {
                "status": "error",
                "source": "himalayas",
                "inserted": 0,
                "message": str(e),
            }

        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
