# api/run.py
import json
import traceback
from http.server import BaseHTTPRequestHandler

from match_and_email import main


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        GET /api/run -> same as /api/match for now.

        Runs the ASAP Jobs matching + digest generation (DRY-RUN send).
        """

        try:
            print("[API] /api/run called – starting automation run")
            main()
            status = 200
            payload = {
                "status": "success",
                "message": "ASAP Jobs automation run completed successfully",
            }
            print("[API] /api/run completed successfully")

        except Exception as e:
            traceback.print_exc()
            print(f"[API] /api/run error: {e}")

            status = 500
            payload = {
                "status": "error",
                "message": str(e),
            }

        body = json.dumps(payload).encode("utf-8")

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
