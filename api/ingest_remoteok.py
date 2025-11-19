# api/ingest_remoteok.py

from http.server import BaseHTTPRequestHandler
import json

from remoteok_ingest import ingest_remoteok


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Vercel Python entrypoint for GET /api/ingest_remoteok

        - Calls ingest_remoteok()
        - Returns JSON with how many jobs were ingested
        """

        try:
            ingested_count = ingest_remoteok()
            status = 200
            payload = {
                "status": "success",
                "source": "remoteok",
                "ingested": ingested_count,
            }
        except Exception as e:
            status = 500
            payload = {
                "status": "error",
                "source": "remoteok",
                "message": str(e),
            }

        body = json.dumps(payload)

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
