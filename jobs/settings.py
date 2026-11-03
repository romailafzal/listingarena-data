"""Concurrency of every step, one place. Defaults follow the reference pipeline; each one is overridable by env."""
from __future__ import annotations

import os


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name) or default)


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name) or default)


# Listings runs: Redfin and Realtor are collected at the same time; states one after another
PIPELINE_GAP_HOURS = _float("PIPELINE_GAP_HOURS", 3)            # break between the end of a run and the next
PIPELINE_STATE_PAUSE_MINUTES = os.environ.get("PIPELINE_STATE_PAUSE_MINUTES", "0")  # "5" or a range "6-7"

# Details: each listing's own page (price events, sales, taxes)
DETAILS_WORKERS = _int("DETAILS_WORKERS", 32)
DETAILS_EVERY_MINUTES = _int("DETAILS_EVERY_MINUTES", 30)

# Photos: downloads at once, at most PHOTOS_RATE a second for all of them, PHOTOS_PER_LISTING kept per listing
PHOTOS_WORKERS = _int("PHOTOS_WORKERS", 8)
PHOTOS_RATE = _float("PHOTOS_RATE", 15)
PHOTOS_UPLOADERS = _int("PHOTOS_UPLOADERS", 32)
PHOTOS_PER_LISTING = _int("PHOTOS_PER_LISTING", 3)
PHOTOS_PAUSE = _int("PHOTOS_PAUSE", 120)                         # seconds every download waits after a refusal
PHOTOS_EVERY_MINUTES = _int("PHOTOS_EVERY_MINUTES", 30)
PHOTOS_DIR = os.environ.get("PHOTOS_DIR", "/fast/exports/photos")
SALE_PHOTOS_STATES = {s.strip().upper() for s in os.environ.get("SALE_PHOTOS_STATES", "").split(",") if s.strip()}

# Floor plans: Redfin rental buildings' units
FLOOR_PLANS_WORKERS = _int("FLOOR_PLANS_WORKERS", 8)
FLOOR_PLANS_RATE = _float("FLOOR_PLANS_RATE", 5)
LISTINGS_FLOOR_PLANS_WORKERS = _int("LISTINGS_FLOOR_PLANS_WORKERS", 16)

# Redfin-only: Realtor ids of listings only Redfin has
REDFIN_ONLY_WORKERS = _int("REDFIN_ONLY_WORKERS", 8)
REDFIN_ONLY_RATE = _float("REDFIN_ONLY_RATE", 5)
REDFIN_ONLY_RETRY_DAYS = _int("REDFIN_ONLY_RETRY_DAYS", 14)
REDFIN_ONLY_EVERY_MINUTES = _int("REDFIN_ONLY_EVERY_MINUTES", 30)
