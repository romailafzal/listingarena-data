"""Photos: download each active listing's first photos and store them once, named after their link.

python -m jobs.photos [--state XX] [--limit N] [--workers N] [--rate N] [--download] [--coverage]

By default only the links are saved (listing_photos rows with no file yet, no requests); --download (or
PHOTOS_DOWNLOAD=1) downloads them, once every listing has its listing data and links.

Which listings: active rentals everywhere, and active sale listings in SALE_PHOTOS_STATES; new ones (no photos yet)
and relisted ones (photos downloaded before the listing was last seen as new). Links come from listings.photo_url
(main photo) and listing_details.photos (the rest, after the details step read the listing's page).
Files: PHOTOS_DIR/<state>/<sha1 of link>.jpg, so units of one building that share photos share the files.
A 404 counts as done; any other failure stays to do and is tried again next run.
Pace: PHOTOS_WORKERS downloads at once, at most PHOTOS_RATE a second for all of them; a 403/429 pauses every
worker PHOTOS_PAUSE seconds (jobs/settings.py).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from jobs import settings  # noqa: E402
from jobs.pace import Pace  # noqa: E402
from sources.http import browser_headers  # noqa: E402

# listings whose photos are due, with their links in order (main photo first, no duplicates)
SELECT_DUE = """
SELECT l.id, l.state, l.photo_url, d.photos
FROM listings l
LEFT JOIN LATERAL (SELECT photos FROM listing_details d WHERE d.listing_id = l.id AND d.photos IS NOT NULL
                   ORDER BY fetched_at DESC LIMIT 1) d ON true
WHERE l.active
  AND (l.listing_type = 'rent' OR (l.listing_type = 'sale' AND l.state = ANY(%(sale_states)s)))
  AND (%(state)s::text IS NULL OR l.state = %(state)s)
  AND (l.photo_url IS NOT NULL OR d.photos IS NOT NULL)
  AND NOT EXISTS (SELECT 1 FROM listing_photos p WHERE p.listing_id = l.id
                  AND (p.downloaded_at >= l.first_seen OR p.download_error = '404'
                       OR (NOT %(download)s AND p.downloaded_at IS NULL)))
ORDER BY l.listing_type, l.id
LIMIT %(limit)s
"""
COVERAGE = """
SELECT l.state, count(*) AS listings, count(*) FILTER (WHERE EXISTS
  (SELECT 1 FROM listing_photos p WHERE p.listing_id = l.id AND p.downloaded_at IS NOT NULL)) AS with_photos
FROM listings l WHERE l.active AND l.listing_type = 'rent' GROUP BY 1 ORDER BY 1
"""


def links(photo_url: str | None, photos, n: int) -> list[str]:
    out = [photo_url] if photo_url else []
    for p in photos or []:
        href = p.get("href") if isinstance(p, dict) else p
        if href and href not in out:
            out.append(href)
    return out[:n]


def storage_key(state: str, url: str) -> str:
    return f"{(state or 'xx').lower()}/{hashlib.sha1(url.encode()).hexdigest()}.jpg"


class PhotoJob:
    def __init__(self, conn, out: Path, pace: Pace, log=print, download: bool = True):
        self.conn, self.out, self.pace, self.log = conn, out, pace, log
        self.download_files = download
        self.session = requests.Session()
        self.stats = {"downloaded": 0, "already": 0, "missing": 0, "errors": 0, "bytes": 0}

    def download(self, url: str, key: str) -> tuple[int | None, str | None]:
        path = self.out / key
        if path.exists():
            self.stats["already"] += 1
            return path.stat().st_size, None
        for attempt in range(4):
            self.pace.wait()
            try:
                r = self.session.get(url, headers={**browser_headers(), "accept": "image/avif,image/webp,image/*,*/*"},
                                     timeout=30)
            except requests.RequestException as e:
                err = type(e).__name__
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 200:
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".part")
                tmp.write_bytes(r.content)
                tmp.replace(path)
                self.stats["downloaded"] += 1
                self.stats["bytes"] += len(r.content)
                return len(r.content), None
            if r.status_code == 404:
                self.stats["missing"] += 1
                return None, "404"
            err = f"HTTP {r.status_code}"
            if r.status_code in (403, 429):
                self.pace.refused(r.status_code)
        self.stats["errors"] += 1
        return None, err

    def listing(self, row) -> list[tuple]:
        lid, state, photo_url, photos = row
        urls = links(photo_url, photos, settings.PHOTOS_PER_LISTING)
        if not self.download_files:
            return [(lid, pos, url, None, None, None) for pos, url in enumerate(urls)]
        rows = []
        for pos, url in enumerate(urls):
            key = storage_key(state, url)
            size, err = self.download(url, key)
            rows.append((lid, pos, url, None if err else key, size, err))
        return rows

    def save(self, rows: list[tuple]) -> None:
        with self.conn.transaction():
            self.conn.cursor().executemany(
                """INSERT INTO listing_photos (listing_id, position, source_url, storage_key, bytes, downloaded_at, download_error)
                   VALUES (%s, %s, %s, %s, %s, CASE WHEN %s::text IS NULL AND %s::text IS NOT NULL THEN now() END, %s)
                   ON CONFLICT (listing_id, position) DO UPDATE SET source_url = EXCLUDED.source_url,
                     storage_key = EXCLUDED.storage_key, bytes = EXCLUDED.bytes,
                     downloaded_at = EXCLUDED.downloaded_at, download_error = EXCLUDED.download_error""",
                [(lid, pos, url, key, size, err, key, err) for lid, pos, url, key, size, err in rows])

    def run(self, due: list, workers: int) -> dict:
        t0 = time.time()
        with ThreadPoolExecutor(workers, thread_name_prefix="photos") as pool:
            for i, rows in enumerate(pool.map(self.listing, due), 1):
                if rows:
                    self.save(rows)
                if i % 500 == 0 or i == len(due):
                    self.log(f"[photos] {i}/{len(due)} listings, {time.time() - t0:.0f}s: {json.dumps(self.stats)}")
        return {"listings": len(due), "seconds": round(time.time() - t0, 1), **self.stats,
                "refusals": self.pace.refusals}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--state")
    ap.add_argument("--limit", type=int, default=200_000)
    ap.add_argument("--workers", type=int, default=settings.PHOTOS_WORKERS)
    ap.add_argument("--rate", type=float, default=settings.PHOTOS_RATE)
    ap.add_argument("--download", action="store_true", default=os.environ.get("PHOTOS_DOWNLOAD") == "1",
                    help="download the files too (default: save the links only)")
    ap.add_argument("--coverage", action="store_true", help="only count the coverage, download nothing")
    a = ap.parse_args(argv)
    from db.migrate import connect
    with connect() as conn:
        conn.autocommit = True
        if a.coverage:
            for r in conn.execute(COVERAGE):
                print(f"{r[0]}  {r[2]}/{r[1]} rentals with photos")
            return 0
        due = conn.execute(SELECT_DUE, {"state": a.state and a.state.upper(), "limit": a.limit,
                                        "sale_states": sorted(settings.SALE_PHOTOS_STATES),
                                        "download": a.download}).fetchall()
        print(f"[photos] {len(due)} listings due (state={a.state or 'all'}, workers={a.workers}, rate={a.rate}/s)",
              flush=True)
        res = PhotoJob(conn, Path(settings.PHOTOS_DIR), Pace(a.rate, settings.PHOTOS_PAUSE, "photos"),
                       log=lambda m: print(m, flush=True), download=a.download).run(
                           due, a.workers if a.download else 1)
    print(json.dumps(res), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
