# api/ingest_remoteok.py
import json
from http.server import BaseHTTPRequestHandler

from remoteok_ingest import ingest_remoteok


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            inserted = ingest_remoteok()
            body = {
                "status": "success",
                "inserted": inserted,
                "message": f"Ingested {inserted} new RemoteOK jobs",
            }
            status_code = 200
        except Exception as e:
            body = {
                "status": "error",
                "message": str(e),
            }
            status_code = 500

        body_bytes = json.dumps(body).encode("utf-8")

        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)
