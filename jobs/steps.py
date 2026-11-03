"""The pipeline's steps, in order, with the names used everywhere (logs, Dagster, the monitor, docs).

Listings run (every state, one after another; each step for every listing type before the next stage):
  collect   search every county on Redfin and Realtor AT THE SAME TIME, merge them by address, record
            new / changed / removed / relisted listings                 -> exports/bronze, exports/silver
  clean     drop site-specific columns, assign our stable ids, add row_hash
  load      upsert into PostgreSQL and append the changes to listing_events, in one transaction

Listing extras (own jobs, apart from the listings runs, each every <JOB>_EVERY_MINUTES):
  details      each listing's own page: price events, sales, taxes, schools        jobs.fetch_details
  redfin_only  Realtor property ids of listings only Redfin has, for details        jobs.redfin_only
  photos       each listing's first photos, downloaded and stored                   jobs.photos
  floor_plans  Redfin rental buildings' units, as often as they change              jobs.floor_plans

Concurrency of every step: jobs/settings.py.
"""
from __future__ import annotations

SCRAPERS = ["redfin", "realtor"]              # collected side by side; one failing is tolerated
STAGES = {
    "collect": ["jobs.collect"],
    "clean": ["normalization"],
    "load": ["db.load"],
}
LISTING_TYPES = ["rent", "sale", "sold"]
EXTRAS = {
    "details": "jobs.fetch_details",
    "redfin_only": "jobs.redfin_only",
    "photos": "jobs.photos",
    "floor_plans": "jobs.floor_plans",
}


def step_label(state: str, stage: str, source: str, listing_type: str) -> str:
    """e.g. TX-collect-redfin-rent: the name a step shows under in logs and on the monitor."""
    return f"{state}-{stage}-{source}-{listing_type}"
