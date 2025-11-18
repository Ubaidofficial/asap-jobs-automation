# api/match.py
import json
from match_and_email import main


def handler(request):
    """
    Vercel serverless entrypoint for running the matching + digest pipeline.

    You can hit:
      https://YOUR-PROJECT.vercel.app/api/match
    or use it from a Vercel Cron Job.
    """
    try:
        main()
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "status": "success",
                "message": "ASAP Jobs matching ran successfully",
            }),
        }
    except Exception as e:
        # Log to console so it shows up in Vercel logs
        print("Error in /api/match:", repr(e))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "status": "error",
                "message": str(e),
            }),
        }
