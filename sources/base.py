"""Source interface. Homes.com, detail pages etc. plug in by subclassing Source."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator

LISTING_TYPES = ("sale", "rent", "sold")


@dataclass(frozen=True)
class County:
    state: str            # two-letter code, e.g. "TX"
    name: str             # "Travis County"
    fips: str             # five digits, "48453"
    redfin_region_id: int | None = None
    realtor_location: str | None = None
    extra: dict = field(default_factory=dict, compare=False)


@dataclass
class Page:
    """One raw response page; saved to bronze verbatim."""
    number: int
    request: dict          # what we sent (url/params/body) for reproducibility
    response: Any          # parsed JSON
    records: list          # raw listing records extracted from the response
    meta: dict = field(default_factory=dict)  # e.g. total, price band


class Source:
    name: str = ""
    workers: int = 2           # default max concurrent counties for this source

    def search(self, county: County, listing_type: str, max_pages: int | None = None) -> Iterator[Page]:
        """Yield raw search-result pages for a county. Use iter_records() for records only."""
        raise NotImplementedError

    def iter_records(self, county: County, listing_type: str, max_pages: int | None = None) -> Iterator[dict]:
        for page in self.search(county, listing_type, max_pages):
            yield from page.records

    def normalize(self, raw: dict, listing_type: str) -> dict:
        """Map a raw record to the silver schema (see normalization.schema.SILVER_FIELDS)."""
        raise NotImplementedError

    def supports(self, county: County) -> bool:
        return True


def fetch_ordered(fn, args: list, parallel: int):
    """Yield fn(arg) for each arg in order, running up to `parallel` calls at once (lazy window).

    Used for parallel page fetches inside one county/type search; the HTTP client's limiter still caps the
    source-wide in-flight total. Exceptions propagate when their result is reached.
    """
    parallel = parallel if isinstance(parallel, int) else 1
    if parallel <= 1 or len(args) <= 1:
        for a in args:
            yield fn(a)
        return
    from collections import deque
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=min(parallel, len(args))) as ex:
        it, q = iter(args), deque()
        for a in it:
            q.append(ex.submit(fn, a))
            if len(q) >= parallel:
                break
        try:
            while q:
                yield q.popleft().result()
                nxt = next(it, None)
                if nxt is not None:
                    q.append(ex.submit(fn, nxt))
        finally:
            for f in q:
                f.cancel()
