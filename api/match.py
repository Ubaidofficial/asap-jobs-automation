# api/match.py
import json
import traceback
from http.server import BaseHTTPRequestHandler

from match_and_email import main


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Vercel Python Serverless Function entrypoint.

        GET /api/match -> runs the matching+email logic (currently DRY-RUN).
        """

        try:
            # Run your main matching job
            main()
            status = 200
            payload = {
                "status": "success",
                "message": "ASAP Jobs matching ran successfully",
            }
        except Exception as e:
            # Log full traceback to Vercel logs
            traceback.print_exc()

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
