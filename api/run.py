# api/run.py
import json
from match_and_email import main


def handler(request):
    """
    Alternate entrypoint for the same ASAP Jobs automation pipeline.
    Useful if you want your Cron to hit /api/run while /api/match is
    kept for manual triggering/testing.
    """
    try:
        main()
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "status": "success",
                "message": "ASAP Jobs automation ran successfully",
            }),
        }
    except Exception as e:
        print("Error in /api/run:", repr(e))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "status": "error",
                "message": str(e),
            }),
        }
