# remote_companies_config.py
"""
List of remote-first companies whose careers pages we crawl directly.

For each company:
- slug: short id (used in source_job_id)
- company: human-readable company name
- ats: which ATS / layout to use ("greenhouse" | "lever")
- careers_url: main jobs board URL
- default_location: fallback if job has no location
"""

REMOTE_COMPANIES = [
    {
        "slug": "gitlab",
        "company": "GitLab",
        "ats": "greenhouse",
        "careers_url": "https://boards.greenhouse.io/gitlab",
        "default_location": "Remote",
    },
    {
        "slug": "zapier",
        "company": "Zapier",
        "ats": "lever",
        "careers_url": "https://jobs.lever.co/zapier",
        "default_location": "Remote",
    },
    {
        "slug": "automattic",
        "company": "Automattic",
        "ats": "greenhouse",
        "careers_url": "https://boards.greenhouse.io/automattic",
        "default_location": "Remote",
    },
    {
        "slug": "basecamp",
        "company": "Basecamp",
        "ats": "greenhouse",
        "careers_url": "https://boards.greenhouse.io/basecamp",
        "default_location": "Remote",
    },
    # 👆 Add more companies here as you grow.
]
