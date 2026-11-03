"""Realtor.com search via the public website GraphQL endpoint (home_search)."""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

from normalization.extras import realtor_extra
from normalization.schema import blank_record, iso_date, num
from sources.base import County, Page, Source, fetch_ordered
from sources.http import SourceClient

ENDPOINT = "https://www.realtor.com/frontdoor/graphql"
HEADERS = {"content-type": "application/json", "rdc-client-name": "RDC_WEB_SRP_FOR_SALE_PAGE",
           "rdc-client-version": "3.0.0", "origin": "https://www.realtor.com", "referer": "https://www.realtor.com/",
           "sec-fetch-site": "same-origin"}
PAGE_SIZE = 200
MAX_RESULTS = 10_000          # offset+limit cap per search; bigger searches are split by price band
STATUS = {"sale": "for_sale", "rent": "for_rent", "sold": "sold"}
SOLD_DAYS = int(os.environ.get("SOLD_WITHIN_DAYS", 90))

FIELDS_FILE = Path(__file__).with_name("realtor_fields.graphql")
FALLBACK_FIELDS = (
    "property_id listing_id href permalink status list_price list_price_min list_price_max price_reduced_amount "
    "list_date last_update_date last_status_change_date last_sold_date last_sold_price photo_count matterport tags "
    "primary_photo{href} photos{href} virtual_tours{href type} open_houses{start_date end_date} pet_policy{cats dogs} "
    "hoa{fee} description{type sub_type name beds baths baths_full baths_half sqft lot_sqft year_built garage stories "
    "text sold_price sold_date} location{address{line city state_code postal_code coordinate{lat lon}} "
    "county{name fips_code}} flags{is_new_listing is_pending is_contingent is_foreclosure is_price_reduced "
    "is_new_construction is_coming_soon} source{id type name listing_id agents{agent_id agent_name office_name}}"
)


def fields() -> str:
    try:
        return FIELDS_FILE.read_text().strip()
    except OSError:
        return FALLBACK_FIELDS


def build_body(location: str, listing_type: str, offset: int, limit: int = PAGE_SIZE,
               price: tuple | None = None, today: date | None = None) -> dict:
    q: dict = {"status": [STATUS[listing_type]], "search_location": {"location": location}}
    sort_field = "list_date"
    if listing_type == "sold":
        q["sold_date"] = {"min": ((today or date.today()) - timedelta(days=SOLD_DAYS)).isoformat()}
        sort_field = "sold_date"
    if price:
        lo, hi = price
        q["list_price" if listing_type != "sold" else "sold_price"] = {"min": lo} if hi is None else {"min": lo, "max": hi}
    query = ("query ConsumerSearchQuery($query:HomeSearchCriteria!,$limit:Int,$offset:Int,$sort:[SearchAPISort]){"
             "home_search(query:$query,limit:$limit,offset:$offset,sort:$sort){count total results{" + fields() + "}}}")
    return {"operationName": "ConsumerSearchQuery", "query": query,
            "variables": {"query": q, "limit": limit, "offset": offset,
                          # listing_id tie-break keeps paging stable (no repeats/skips)
                          "sort": [{"field": sort_field, "direction": "desc"}, {"field": "listing_id", "direction": "asc"}]}}


def parse_search(data: dict) -> tuple[list, int]:
    """Return (results, total). Raises if the response carries no home_search."""
    hs = ((data or {}).get("data") or {}).get("home_search")
    if hs is None:
        raise ValueError(f"no home_search in response: {(data or {}).get('errors')}")
    return hs.get("results") or [], hs.get("total") or 0


class RealtorSource(Source):
    name = "realtor"
    workers = 2

    def __init__(self, client: SourceClient | None = None):
        self.client = client or SourceClient("realtor", min_interval=2.0)

    def supports(self, county: County) -> bool:
        return bool(county.realtor_location)

    @staticmethod
    def listing_key(raw: dict) -> str:
        """home_search `total` counts listings: one home listed by two MLS feeds (e.g. AUTX + CTTX) is two
        results with the same property_id. Count completeness on property_id+listing_id."""
        return f"{raw.get('property_id')}:{raw.get('listing_id')}"

    def _fetch(self, body: dict) -> dict:
        last = None
        for _ in range(3):  # empty/errored home_search is transient
            data = self.client.request_json("POST", ENDPOINT, headers=HEADERS, json=body)
            try:
                parse_search(data)
                return data
            except ValueError as e:
                last = e
                self.client.sleep(5)
        raise RuntimeError(f"realtor: {last}")

    def _total(self, location, listing_type, price) -> int:
        return parse_search(self._fetch(build_body(location, listing_type, 0, limit=1, price=price)))[1]

    def _bands(self, location, listing_type, lo=0, hi=None) -> list:
        """Split (lo, hi) by price until each band has <= MAX_RESULTS; both halves are probed in parallel."""
        total = self._total(location, listing_type, (lo, hi))
        if total <= MAX_RESULTS or (hi is not None and hi - lo < 1):
            return [(lo, hi)]
        mid = (lo + hi) / 2 if hi is not None else max(lo * 2, 1_000_000 if listing_type != "rent" else 5_000)
        halves = [(lo, mid), (mid, hi)]
        out = []
        for part in fetch_ordered(lambda b: self._bands(location, listing_type, *b), halves,
                                  2 if isinstance(self.client.page_parallel, int) and self.client.page_parallel > 1 else 1):
            out += part
        return out

    def search(self, county: County, listing_type: str, max_pages: int | None = None) -> Iterator[Page]:
        counter = [0]
        total = 0
        for page in self._continue(county.realtor_location, listing_type, None, counter, max_pages, 0):
            total = page.meta["total"]
            yield page
        if total <= MAX_RESULTS:
            return
        # > 10k: the newest 10k are in; price bands bring the rest (bands overlap at edges; dedupe by property_id)
        for band in self._bands(county.realtor_location, listing_type):
            if max_pages is not None and counter[0] >= max_pages:
                return
            yield from self._continue(county.realtor_location, listing_type, band, counter, max_pages, 0)

    def _continue(self, location, listing_type, price, counter, max_pages, offset) -> Iterator[Page]:
        """First page sequentially (gives total); remaining offsets up to the 10k cap fetched
        `client.page_parallel` at a time, yielded in order."""
        if max_pages is not None and counter[0] >= max_pages:
            return
        body = build_body(location, listing_type, offset, price=price)
        data = self._fetch(body)
        results, total = parse_search(data)
        if offset >= total and not results:
            return
        counter[0] += 1
        yield Page(counter[0], {"url": ENDPOINT, "body": body["variables"]}, data, results,
                   {"total": total, "offset": offset, "price_band": price})
        if not results:
            return
        offsets = list(range(offset + PAGE_SIZE, min(total, MAX_RESULTS - PAGE_SIZE + 1), PAGE_SIZE))
        if max_pages is not None:
            offsets = offsets[:max(0, max_pages - counter[0])]
        bodies = [build_body(location, listing_type, o, price=price) for o in offsets]
        for o, b, data in zip(offsets, bodies, fetch_ordered(self._fetch, bodies, self.client.page_parallel)):
            results, total2 = parse_search(data)
            counter[0] += 1
            yield Page(counter[0], {"url": ENDPOINT, "body": b["variables"]}, data, results,
                       {"total": total2, "offset": o, "price_band": price})
            if not results:
                return

    def normalize(self, raw: dict, listing_type: str) -> dict:
        r = raw
        loc = r.get("location") or {}
        a = loc.get("address") or {}
        c = a.get("coordinate") or {}
        d = r.get("description") or {}
        rec = blank_record()
        price = r.get("list_price") if r.get("list_price") is not None else r.get("list_price_min")
        sold_price = d.get("sold_price") or r.get("last_sold_price")
        units = r.get("units") or []
        rec.update({
            "listing_type": listing_type,
            "external_id": f"realtor:{r.get('property_id')}",
            "address": a.get("line"), "city": a.get("city"), "state": a.get("state_code"), "zip": a.get("postal_code"),
            "property_type": d.get("type"),
            "beds": num(d.get("beds") if d.get("beds") is not None else d.get("beds_min")),
            "baths": num(d.get("baths") if d.get("baths") is not None else d.get("baths_min")),
            "sqft": num(d.get("sqft") if d.get("sqft") is not None else d.get("sqft_min")),
            "lot_sqft": num(d.get("lot_sqft")), "year_built": num(d.get("year_built")),
            "price": num(sold_price if listing_type == "sold" else price),
            "list_price": num(price),
            "sold_date": iso_date(d.get("sold_date") or r.get("last_sold_date")) if listing_type == "sold" else None,
            "listed_date": iso_date(r.get("list_date")),
            "available_from": next((iso_date((u.get("availability") or {}).get("date")) for u in units
                                    if (u.get("availability") or {}).get("date")), None) if listing_type == "rent" else None,
            "deposit": None,
            "lat": c.get("lat"), "lng": c.get("lon"),
            "description": d.get("text"),
            "photo_url": (r.get("primary_photo") or {}).get("href"),
            "status": r.get("status"),
            "source": self.name, "source_id": str(r.get("property_id")),
            "url": r.get("href") or (f"https://www.realtor.com/realestateandhomes-detail/{r['permalink']}" if r.get("permalink") else None),
            "extra": realtor_extra(r),
        })
        return rec
