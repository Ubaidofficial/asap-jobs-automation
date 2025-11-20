# api/ingest_all.py

from http.server import BaseHTTPRequestHandler
import json
import logging

from remoteok_ingest import ingest_remoteok
from remotive_ingest import ingest_remotive
from weworkremotely_ingest import ingest_weworkremotely
from himalayas_ingest import ingest_himalayas
from remoteco_ingest import ingest_remoteco
from remote_companies_ingest import ingest_remote_companies

logger = logging.getLogger("api.ingest_all")
logger.setLevel(logging.INFO)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        GET /api/ingest_all

        Runs all ingestors:
        - RemoteOK
        - Remotive
        - WeWorkRemotely
        - Himalayas
        - Remote.co
        - Remote-first companies

        All of them only insert Remote / Hybrid jobs based on remote_scope.
        Returns a JSON object with per-source results.
        """

        sources = {}

        # RemoteOK
        try:
            inserted_remoteok = ingest_remoteok()
            sources["remoteok"] = {
                "status": "success",
                "inserted": inserted_remoteok,
            }
        except Exception as e:
            logger.exception("Error ingesting RemoteOK: %s", e)
            sources["remoteok"] = {
                "status": "error",
                "inserted": 0,
                "error": str(e),
            }

        # Remotive
        try:
            inserted_remotive = ingest_remotive()
            sources["remotive"] = {
                "status": "success",
                "inserted": inserted_remotive,
            }
        except Exception as e:
            logger.exception("Error ingesting Remotive: %s", e)
            sources["remotive"] = {
                "status": "error",
                "inserted": 0,
                "error": str(e),
            }

        # WeWorkRemotely
        try:
            inserted_wwr = ingest_weworkremotely()
            sources["weworkremotely"] = {
                "status": "success",
                "inserted": inserted_wwr,
            }
        except Exception as e:
            logger.exception("Error ingesting WeWorkRemotely: %s", e)
            sources["weworkremotely"] = {
                "status": "error",
                "inserted": 0,
                "error": str(e),
            }

        # Himalayas
        try:
            inserted_himalayas = ingest_himalayas()
            sources["himalayas"] = {
                "status": "success",
                "inserted": inserted_himalayas,
            }
        except Exception as e:
            logger.exception("Error ingesting Himalayas: %s", e)
            sources["himalayas"] = {
                "status": "error",
                "inserted": 0,
                "error": str(e),
            }

        # Remote.co
        try:
            inserted_remoteco = ingest_remoteco()
            sources["remoteco"] = {
                "status": "success",
                "inserted": inserted_remoteco,
            }
        except Exception as e:
            logger.exception("Error ingesting Remote.co: %s", e)
            sources["remoteco"] = {
                "status": "error",
                "inserted": 0,
                "error": str(e),
            }

        # Remote-first companies
        try:
            inserted_rc = ingest_remote_companies()
            sources["remote_companies"] = {
                "status": "success",
                "inserted": inserted_rc,
            }
        except Exception as e:
            logger.exception("Error ingesting remote companies: %s", e)
            sources["remote_companies"] = {
                "status": "error",
                "inserted": 0,
                "error": str(e),
            }

        # Overall status
        overall_status = "success"
        for s in sources.values():
            if s["status"] != "success":
                overall_status = "partial"
                break

        payload = {
            "status": overall_status,
            "message": "Ingested from all sources (see per-source results).",
            "sources": sources,
        }

        body = json.dumps(payload).encode("utf-8")

        self.send_response(200 if overall_status == "success" else 207)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
