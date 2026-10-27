"""Redfin search via the public website "stingray" endpoints (gis for sale/sold, rentals for rent)."""
from __future__ import annotations

import os
from typing import Iterator
from urllib.parse import quote, urlencode

from normalization.schema import blank_record, iso_date, ms_to_date, num
from sources.base import County, Page, Source
from sources.http import SourceClient

GIS = "https://www.redfin.com/stingray/api/gis"
RENTALS = "https://www.redfin.com/stingray/api/v1/search/rentals"
PER_PAGE = 350                 # most the endpoints return per request
SOLD_DAYS = int(os.environ.get("SOLD_WITHIN_DAYS", 90))
HEADERS = {"referer": "https://www.redfin.com/", "sec-fetch-site": "same-origin"}

# Redfin propertyType codes
PROPERTY_TYPES = {1: "house", 2: "condo", 3: "condo", 4: "multi_family", 5: "apartment", 6: "single_family",
                  7: "mobile", 8: "land", 11: "other", 13: "townhouse"}


def area_params(county: County, listing_type: str) -> list[dict]:
    polys = county.extra.get("redfin_rental_polygon") if listing_type == "rent" else None
    if polys:  # rentals search 500s for a few counties; search their outline instead
        return [{"poly": ",".join(f"{lon} {lat}" for lon, lat in p)} for p in polys]
    return [{"region_id": county.redfin_region_id, "region_type": 5}]


def build_url(area: dict, listing_type: str, page: int, per_page: int = PER_PAGE) -> str:
    p = {"al": 1, "num_homes": per_page, "ord": "redfin-recommended-asc", "page_number": page,
         "start": (page - 1) * per_page, "uipt": "1,2,3,4,5,6,7,8", "v": 8, **area}
    if listing_type == "sale":
        p.update(sf="1,2,3,5,6,7", status=9)            # active + coming soon + pending, all listing sources
        return f"{GIS}?{urlencode(p, quote_via=quote, safe=',')}"
    if listing_type == "sold":
        p.update(status=9, mpt=99, sold_within_days=SOLD_DAYS)  # no sf + mpt=99 => sold homes
        return f"{GIS}?{urlencode(p, quote_via=quote, safe=',')}"
    p.update(isRentals="true", status=9, sf="1,2,3,5,6,7", use_max_pins="true")
    return f"{RENTALS}?{urlencode(p, quote_via=quote, safe=',')}"


def parse_response(data: dict, listing_type: str) -> tuple[list, int | None]:
    """Return (homes, total-if-known). Raises ValueError on Redfin's in-band errors."""
    if listing_type == "rent":
        if "homes" not in data:
            raise ValueError(f"rentals: unexpected response keys {list(data)[:5]}")
        return data.get("homes") or [], data.get("numMatchedHomes")
    if data.get("resultCode") not in (0, None):
        raise ValueError(f"gis error {data.get('resultCode')}: {data.get('errorMessage')}")
    return (data.get("payload") or {}).get("homes") or [], None


def property_id(home: dict, listing_type: str) -> str:
    return str(home["homeData"]["propertyId"] if listing_type == "rent" else home["propertyId"])


def _v(x):
    return x.get("value") if isinstance(x, dict) else x


def photo_url(h: dict) -> str | None:
    """Primary MLS photo on Redfin's CDN: /photo/<dataSourceId>/bigphoto/<last 3 of mls#>/<mls#>_0.jpg."""
    mls, ds = _v(h.get("mlsId")), h.get("dataSourceId")
    if not mls or ds is None or not h.get("numPictures"):
        return None
    return f"https://ssl.cdn-redfin.com/photo/{ds}/bigphoto/{str(mls)[-3:]}/{mls}_0.jpg"


class RedfinSource(Source):
    name = "redfin"
    workers = 2

    def __init__(self, client: SourceClient | None = None):
        self.client = client or SourceClient("redfin", min_interval=1.5)

    def supports(self, county: County) -> bool:
        return bool(county.redfin_region_id) or bool(county.extra.get("redfin_rental_polygon"))

    def _fetch(self, url: str, listing_type: str):
        last = None
        for attempt in range(3):
            data = self.client.request_json("GET", url, headers=HEADERS)
            try:
                return data, parse_response(data or {}, listing_type)
            except ValueError as e:  # e.g. "GIS Search Service timeout error": transient
                last = e
                self.client.sleep(15 * (attempt + 1))
        raise RuntimeError(f"redfin: {last}")

    def search(self, county: County, listing_type: str, max_pages: int | None = None) -> Iterator[Page]:
        n = 0
        for area in area_params(county, listing_type):
            seen: set[str] = set()
            page = 1
            while max_pages is None or n < max_pages:
                url = build_url(area, listing_type, page)
                data, (homes, total) = self._fetch(url, listing_type)
                new = [h for h in homes if property_id(h, listing_type) not in seen]
                seen.update(property_id(h, listing_type) for h in new)
                n += 1
                yield Page(n, {"url": url}, data, new, {"total": total, "returned": len(homes)})
                # pages are not always full; stop on empty page, page of repeats, or reported total
                if not homes or not new or (total is not None and len(seen) >= total):
                    break
                page += 1

    def normalize(self, raw: dict, listing_type: str) -> dict:
        return normalize_rental(raw) if listing_type == "rent" else normalize_gis(raw, listing_type)


def normalize_gis(h: dict, listing_type: str) -> dict:
    ll = _v(h.get("latLong")) or {}
    rec = blank_record()
    price = num(h.get("price"))
    rec.update({
        "listing_type": listing_type,
        "external_id": f"redfin:{h.get('propertyId')}",
        "address": " ".join(x for x in (_v(h.get("streetLine")), _v(h.get("unitNumber"))) if x) or None,
        "city": h.get("city"), "state": h.get("state"), "zip": h.get("zip") or _v(h.get("postalCode")),
        "property_type": PROPERTY_TYPES.get(h.get("propertyType"), h.get("propertyType")),
        "beds": num(h.get("beds")), "baths": num(h.get("baths")), "sqft": num(h.get("sqFt")),
        "lot_sqft": num(h.get("lotSize")), "year_built": num(h.get("yearBuilt")),
        "price": price,
        "list_price": None if listing_type == "sold" else price,
        "sold_date": ms_to_date(h.get("soldDate")) if listing_type == "sold" else None,
        "listed_date": None,
        "lat": ll.get("latitude"), "lng": ll.get("longitude"),
        "description": h.get("listingRemarks"),
        "photo_url": photo_url(h),
        "status": h.get("mlsStatus"),
        "source": "redfin", "source_id": str(h.get("propertyId")),
        "url": "https://www.redfin.com" + h["url"] if h.get("url") else None,
    })
    return rec


def normalize_rental(home: dict) -> dict:
    d = home.get("homeData") or {}
    r = home.get("rentalExtension") or {}
    a = d.get("addressInfo") or {}
    cen = (a.get("centroid") or {}).get("centroid") or {}
    rng = lambda k: (r.get(k) or {}).get("min")  # noqa: E731
    rec = blank_record()
    rec.update({
        "listing_type": "rent",
        "external_id": f"redfin:{d.get('propertyId')}",
        "address": " ".join(x for x in (a.get("formattedStreetLine"), a.get("unitNumber")) if x) or None,
        "city": a.get("city"), "state": a.get("state"), "zip": a.get("zip"),
        "property_type": PROPERTY_TYPES.get(d.get("propertyType"), d.get("propertyType")),
        "beds": num(rng("bedRange")), "baths": num(rng("bathRange")), "sqft": num(rng("sqftRange")),
        "lot_sqft": None, "year_built": None,
        "price": num(rng("rentPriceRange")), "list_price": num(rng("rentPriceRange")),
        "available_from": iso_date(r.get("dateAvailable")),
        "listed_date": None,
        "lat": cen.get("latitude"), "lng": cen.get("longitude"),
        "description": r.get("description"),
        "photo_url": None,
        "status": "for_rent",  # rentalExtension.status is a numeric code
        "source": "redfin", "source_id": str(d.get("propertyId")),
        "url": "https://www.redfin.com" + d["url"] if d.get("url") else None,
    })
    return rec
