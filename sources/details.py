"""Phase 2: per-home DETAIL fetchers (Redfin stingray home/details, Realtor GraphQL `home`).

Per-home endpoints block at far lower concurrency than search (10 workers x 20 in flight got blocked), so each
detail source has its OWN SourceClient (`redfin_detail`, `realtor_detail`) with low defaults:
  <NAME>_DETAIL_MAX_INFLIGHT (default 3; adaptive 3 -> 1 -> 1 after 3 blocks in 2 min, +1 level after 30 quiet min),
  <NAME>_DETAIL_MIN_INTERVAL (default 1.0 s between request starts), connect/read timeouts 10 s / 20 s, 3 attempts.

`fetch()` returns (raw, detail). `raw` goes to bronze verbatim. `detail` is a flat dict: the DETAIL_COLUMNS
(one column each in listing_details) plus 1-to-many lists `price_history`, `tax_history`, `schools`, `comps`
(child tables) and jsonb sections. Parsing never raises on missing keys; a non-JSON / empty / in-band-error
response raises DetailError so the job logs it, counts it as an error and skips the home.
"""
from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timezone
from typing import Any

from normalization.schema import iso_date, num
from sources.http import AdaptiveLimiter, SourceClient, parse_json
from sources.realtor import ENDPOINT as REALTOR_ENDPOINT
from sources.realtor import HEADERS as REALTOR_HEADERS

# scalar columns of listing_details (besides keys / bookkeeping)
DETAIL_COLUMNS = (
    "source_listing_id", "url", "status", "list_price", "list_date", "last_sold_price", "last_sold_date",
    "description", "property_type", "beds", "baths", "baths_full", "baths_half", "sqft", "lot_sqft", "year_built",
    "stories", "garage", "parking", "heating", "cooling", "construction", "style", "hoa_fee", "tax_annual", "tax_year",
    "assessed_value", "property_tax_rate", "lat", "lng", "apn", "neighborhood", "school_district",
    "mls_name", "mls_id", "mls_listing_id", "list_office", "list_office_phone", "list_office_email", "list_broker",
    "list_agent_name", "list_agent_phone", "list_agent_email", "list_agent_license", "buyer_agent_name",
    "buyer_office", "photo_count",
)
# jsonb sections of listing_details
DETAIL_JSON = ("photos", "virtual_tours", "features", "interior_features", "exterior_features", "appliances",
               "agents", "mortgage", "estimates", "open_houses", "flags", "extra")
DETAIL_LISTS = ("price_history", "tax_history", "schools", "comps")      # child tables
TIMEOUT = (10, 20)            # (connect, read) seconds


class DetailError(RuntimeError):
    """Unusable detail response (non-JSON, empty, in-band error). The home is skipped and counted as an error."""


def detail_client(name: str, inflight: int = 3, min_interval: float = 1.0) -> SourceClient:
    """Own low-concurrency client for a per-home endpoint; env <NAME>_DETAIL_MAX_INFLIGHT / _MIN_INTERVAL override."""
    cname = f"{name}_detail"
    c = SourceClient(cname, min_interval=min_interval, retries=3, timeout=TIMEOUT, block_backoff=90.0)
    cap = int(os.environ.get(f"{cname.upper()}_MAX_INFLIGHT", 0) or inflight)
    c.limiter = AdaptiveLimiter(cname, [cap, max(1, cap // 2), 1], trip=3, window=120.0, recover=1800.0)
    return c


def blank_detail() -> dict:
    d = {k: None for k in DETAIL_COLUMNS}
    d.update({k: None for k in DETAIL_JSON})
    d.update({k: [] for k in DETAIL_LISTS})
    return d


def _json(text: str | None, what: str) -> Any:
    if not text or not text.strip():
        raise DetailError(f"{what}: empty response")
    try:
        return parse_json(text)
    except ValueError as e:
        raise DetailError(f"{what}: non-JSON response ({e}): {text[:120]!r}") from None


def _v(x):
    return x.get("value") if isinstance(x, dict) else x


def _int(v):
    n = num(v)
    return int(n) if n is not None else None


def _money(v):
    if isinstance(v, dict):
        v = v.get("amount", v.get("value"))
    return num(v)


def _ms_date(ms) -> str | None:
    if isinstance(ms, str) and not ms.isdigit():
        return iso_date(ms)
    try:
        return datetime.fromtimestamp(float(ms) / 1000, tz=timezone.utc).date().isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def haversine_mi(lat1, lng1, lat2, lng2) -> float | None:
    if None in (lat1, lng1, lat2, lng2):
        return None
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lng2 - lng1) / 2) ** 2
    return round(3958.8 * 2 * math.asin(math.sqrt(a)), 3)


def _find(obj, keys: set, depth: int = 0):
    """First non-empty value of any of `keys` anywhere in obj (bounded depth)."""
    if depth > 8:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and v not in (None, "", {}, []):
                return v
        obj = list(obj.values())
    if isinstance(obj, list):
        for v in obj:
            if isinstance(v, (dict, list)):
                r = _find(v, keys, depth + 1)
                if r is not None:
                    return r
    return None


def _split_camel(s: str) -> str:
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)


def _feature_lists(features: dict) -> dict:
    """{category: ["Key: a, b", "Flag"]} -> columns heating/cooling/construction/style/parking + feature lists."""
    kv: dict[str, str] = {}
    for items in features.values():
        for t in items:
            if isinstance(t, str) and ": " in t:
                k, v = t.split(": ", 1)
                kv.setdefault(k.strip().lower(), v.strip())

    def get(*names):
        return next((kv[n] for n in names if kv.get(n)), None)

    def cat(*names):
        out = []
        for c, items in features.items():
            if c.lower() in names:
                out += [_split_camel(t) for t in items if isinstance(t, str)]
        return out or None
    return {
        "heating": get("heating features", "heating", "heating type"),
        "cooling": get("cooling features", "cooling", "cooling type"),
        "construction": get("construction materials", "construction", "exterior construction"),
        "style": get("architectural style", "style", "property style"),
        "parking": get("parking features", "parking", "garage spaces"),
        "interior_features": cat("interior features", "interior"),
        "exterior_features": cat("exterior and lot features", "exterior features", "exterior", "pool and spa"),
        "appliances": cat("appliances"),
    }


# ---------------------------------------------------------------- Realtor
# Field set verified live 2026-11-02 (GraphQL validates every field; this query returns no errors).
REALTOR_HOME_FIELDS = (
    "property_id listing_id href status list_price list_date last_update_date last_sold_price last_sold_date "
    "photo_count price_per_sqft "
    "description{text beds baths baths_full baths_half sqft lot_sqft year_built type sub_type stories garage} "
    "hoa{fee} photos{href tags{label}} virtual_tours{href type} videos{href} matterport{videos{href}} "
    "open_houses{start_date end_date description} flags{is_foreclosure is_new_construction is_pending is_contingent "
    "is_coming_soon is_price_reduced} pet_policy{cats dogs} "
    "property_history{date event_name price price_sqft source_name source_listing_id listing{list_price status}} "
    "tax_history{year tax assessment{total building land} market{total building land}} "
    "tax_record{apn public_record_id tax_parcel_id} "
    "mortgage{property_tax_rate insurance_rate rates_url estimate{loan_amount monthly_payment total_payment "
    "down_payment average_rate{rate loan_type{term}} monthly_payment_details{type amount display_name}}} "
    "estimates{current_values{estimate estimate_high estimate_low date source{name type}}} "
    "schools{schools{name rating distance_in_miles education_levels funding_type grades district{name id}}} "
    "details{category text} "
    "location{address{line city state_code postal_code coordinate{lat lon}} county{name fips_code} "
    "neighborhoods{name level id} street_view_url} "
    "source{id name type listing_id agents{agent_id agent_name office_name type}} "
    "advertisers{type name email phones{number type} office{name email phones{number}} broker{name} "
    "builder{name} state_license} buyers{name} consumer_advertisers{name phone type agent_id} "
    "related_homes(query:{type:similar_homes}){count results{property_id listing_id status list_price href "
    "last_sold_price last_sold_date description{beds baths sqft lot_sqft year_built type} "
    "location{address{line city state_code postal_code coordinate{lat lon}}}}}"
)


def realtor_body(property_id: str) -> dict:
    return {"operationName": "GetHome",
            "query": "query GetHome($property_id: ID!){home(property_id:$property_id){" + REALTOR_HOME_FIELDS + "}}",
            "variables": {"property_id": str(property_id)}}


def _phone(phones) -> str | None:
    return next((p.get("number") for p in phones or [] if isinstance(p, dict) and p.get("number")), None)


def normalize_realtor(home: dict) -> dict:
    h = home or {}
    desc = h.get("description") or {}
    loc = h.get("location") or {}
    coord = ((loc.get("address") or {}).get("coordinate")) or {}
    src = h.get("source") or {}
    advs = [a for a in h.get("advertisers") or [] if isinstance(a, dict)]
    seller = next((a for a in advs if a.get("type") == "seller"), advs[0] if advs else {})
    office = seller.get("office") or {}
    buyer_agent = next((a for a in src.get("agents") or [] if a.get("type") == "buyer"), None)
    taxes = [{"year": _int(t.get("year")), "tax": num(t.get("tax")),
              "assessed_total": num((t.get("assessment") or {}).get("total")),
              "assessed_building": num((t.get("assessment") or {}).get("building")),
              "assessed_land": num((t.get("assessment") or {}).get("land")),
              "market_total": num((t.get("market") or {}).get("total"))} for t in h.get("tax_history") or []]
    taxes.sort(key=lambda t: t["year"] or 0, reverse=True)
    latest = next((t for t in taxes if t["tax"] is not None), {})
    features = {d.get("category") or "": d.get("text") or [] for d in h.get("details") or [] if isinstance(d, dict)}
    schools = [s for s in ((h.get("schools") or {}).get("schools") or []) if isinstance(s, dict)]
    districts = [((s.get("district") or {}).get("name")) for s in schools]
    lat, lng = num(coord.get("lat")), num(coord.get("lon"))
    comps = []
    for i, c in enumerate(((h.get("related_homes") or {}).get("results") or []), 1):
        cd, cc = c.get("description") or {}, (((c.get("location") or {}).get("address")) or {})
        cco = cc.get("coordinate") or {}
        comps.append({"rank": i, "kind": "similar", "comp_source_id": c.get("property_id"),
                      "comp_listing_id": c.get("listing_id"), "status": c.get("status"),
                      "price": num(c.get("list_price")) or num(c.get("last_sold_price")),
                      "sold_date": iso_date(c.get("last_sold_date")), "beds": num(cd.get("beds")),
                      "baths": num(cd.get("baths")), "sqft": num(cd.get("sqft")), "address": cc.get("line"),
                      "zip": cc.get("postal_code"), "lat": num(cco.get("lat")), "lng": num(cco.get("lon")),
                      "distance_mi": haversine_mi(lat, lng, num(cco.get("lat")), num(cco.get("lon"))),
                      "url": c.get("href")})
    tours = [{"type": t.get("type") or "virtual_tour", "href": t.get("href")} for t in h.get("virtual_tours") or []]
    tours += [{"type": "video", "href": v.get("href")} for v in h.get("videos") or []]
    tours += [{"type": "matterport", "href": v.get("href")} for v in ((h.get("matterport") or {}).get("videos") or [])]
    m = h.get("mortgage") or {}
    neigh = [n for n in loc.get("neighborhoods") or [] if isinstance(n, dict)]
    d = blank_detail()
    d.update({
        "source_listing_id": h.get("listing_id"), "url": h.get("href"), "status": h.get("status"),
        "list_price": num(h.get("list_price")), "list_date": iso_date(h.get("list_date")),
        "last_sold_price": num(h.get("last_sold_price")), "last_sold_date": iso_date(h.get("last_sold_date")),
        "description": desc.get("text"), "property_type": desc.get("sub_type") or desc.get("type"),
        "beds": num(desc.get("beds")), "baths": num(desc.get("baths")), "baths_full": num(desc.get("baths_full")),
        "baths_half": num(desc.get("baths_half")), "sqft": num(desc.get("sqft")), "lot_sqft": num(desc.get("lot_sqft")),
        "year_built": _int(desc.get("year_built")), "stories": num(desc.get("stories")), "garage": num(desc.get("garage")),
        "hoa_fee": num((h.get("hoa") or {}).get("fee")),
        "tax_annual": latest.get("tax"), "tax_year": latest.get("year"), "assessed_value": latest.get("assessed_total"),
        "property_tax_rate": num(m.get("property_tax_rate")),
        "lat": lat, "lng": lng, "apn": (h.get("tax_record") or {}).get("apn"),
        "neighborhood": (neigh[0].get("name") if neigh else None),
        "school_district": next((x for x in districts if x), None),
        "mls_name": src.get("name"), "mls_id": src.get("id"), "mls_listing_id": src.get("listing_id"),
        "list_office": office.get("name"), "list_office_phone": _phone(office.get("phones")),
        "list_office_email": office.get("email"), "list_broker": (seller.get("broker") or {}).get("name"),
        "list_agent_name": seller.get("name"), "list_agent_phone": _phone(seller.get("phones")),
        "list_agent_email": seller.get("email"), "list_agent_license": seller.get("state_license"),
        "buyer_agent_name": (buyer_agent or {}).get("agent_name") or
                            next((b.get("name") for b in h.get("buyers") or [] if isinstance(b, dict)), None),
        "buyer_office": (buyer_agent or {}).get("office_name"),
        "photo_count": _int(h.get("photo_count")),
        "photos": [{"href": p["href"], "tags": [t.get("label") for t in p.get("tags") or [] if isinstance(t, dict)]}
                   for p in h.get("photos") or [] if isinstance(p, dict) and p.get("href")],
        "virtual_tours": [t for t in tours if t.get("href")] or None,
        "features": features or None,
        "agents": {"advertisers": advs, "source_agents": src.get("agents"), "buyers": h.get("buyers"),
                   "consumer_advertisers": h.get("consumer_advertisers")},
        "mortgage": m or None,
        "estimates": (h.get("estimates") or {}).get("current_values"),
        "open_houses": h.get("open_houses"), "flags": h.get("flags"),
        "extra": {"price_per_sqft": num(h.get("price_per_sqft")), "pet_policy": h.get("pet_policy"),
                  "last_update_date": h.get("last_update_date"), "neighborhoods": neigh or None,
                  "public_record_id": (h.get("tax_record") or {}).get("public_record_id"),
                  "county": (loc.get("county") or {}).get("name"), "street_view_url": loc.get("street_view_url"),
                  "comps_total": (h.get("related_homes") or {}).get("count")},
        "price_history": [{"date": iso_date(e.get("date")), "event": e.get("event_name"), "price": num(e.get("price")),
                           "price_sqft": num(e.get("price_sqft")), "source_name": e.get("source_name"),
                           "mls_listing_id": e.get("source_listing_id"),
                           "status": (e.get("listing") or {}).get("status")}
                          for e in h.get("property_history") or [] if isinstance(e, dict)],
        "tax_history": taxes,
        "schools": [{"name": s.get("name"), "rating": num(s.get("rating")), "distance_mi": num(s.get("distance_in_miles")),
                     "levels": s.get("education_levels"), "grades": s.get("grades"), "funding": s.get("funding_type"),
                     "district": (s.get("district") or {}).get("name")} for s in schools],
        "comps": comps,
    })
    d.update(_feature_lists(features))
    return d


class RealtorDetail:
    name = "realtor"

    def __init__(self, client: SourceClient | None = None):
        self.client = client or detail_client("realtor")

    def fetch(self, source_id: str, source_listing_id: str | None = None) -> tuple[dict, dict]:
        r = self.client.request("POST", REALTOR_ENDPOINT, headers=REALTOR_HEADERS, json=realtor_body(source_id))
        data = _json(r.text, "realtor home")
        if not isinstance(data, dict):
            raise DetailError(f"realtor home: unexpected JSON type {type(data).__name__}")
        home = (data.get("data") or {}).get("home")
        if not home:
            raise DetailError(f"realtor home: HTTP {r.status_code}, no home for {source_id}: "
                              f"{json.dumps(data.get('errors'))[:200]}")
        return data, normalize_realtor(home)


# ---------------------------------------------------------------- Redfin
# NOT verified live: this IP gets CloudFront 403 on /stingray/api/home/details/* (2026-11-02) while the search
# endpoints still work. Paths follow the public page's aboveTheFold / belowTheFold payloads; every lookup is
# defensive, so a changed path yields NULL columns, not a crash.
REDFIN_BASE = "https://www.redfin.com/stingray/api/home/details"
REDFIN_HEADERS = {"referer": "https://www.redfin.com/", "sec-fetch-site": "same-origin"}


def redfin_urls(property_id: str, listing_id: str | None) -> dict[str, str]:
    q = f"propertyId={property_id}&accessLevel=1" + (f"&listingId={listing_id}" if listing_id else "")
    return {"aboveTheFold": f"{REDFIN_BASE}/aboveTheFold?{q}", "belowTheFold": f"{REDFIN_BASE}/belowTheFold?{q}"}


def _redfin_payload(data: Any, what: str) -> dict:
    if not isinstance(data, dict):
        raise DetailError(f"{what}: unexpected JSON type {type(data).__name__}")
    if data.get("resultCode") not in (0, None):
        raise DetailError(f"{what}: resultCode {data.get('resultCode')}: {data.get('errorMessage')}")
    p = data.get("payload")
    if not isinstance(p, dict):
        raise DetailError(f"{what}: no payload")
    return p


def _redfin_amenities(b: dict) -> dict:
    """amenitiesInfo.superGroups[].amenityGroups[] -> {groupTitle: ["Name: v1, v2", ...]} (same shape as Realtor)."""
    out: dict[str, list] = {}
    for sg in (b.get("amenitiesInfo") or {}).get("superGroups") or []:
        for g in sg.get("amenityGroups") or []:
            items = []
            for e in g.get("amenityEntries") or []:
                vals = e.get("amenityValues") or []
                name = e.get("amenityName")
                items.append(f"{name}: {', '.join(map(str, vals))}" if name and vals else (name or ", ".join(map(str, vals))))
            if items:
                out.setdefault(g.get("groupTitle") or sg.get("titleString") or "", []).extend(items)
    return out


def normalize_redfin(above: dict | None, below: dict | None) -> dict:
    """above/below = the `payload` objects of aboveTheFold / belowTheFold (either may be None)."""
    a, b = above or {}, below or {}
    addr = a.get("addressSectionInfo") or {}
    main = a.get("mainHouseInfo") or {}
    pub = b.get("publicRecordsInfo") or {}
    basic = pub.get("basicInfo") or {}
    tax = pub.get("taxInfo") or {}
    ll = _v(addr.get("latLong")) or {}
    photos = []
    for p in (a.get("mediaBrowserInfo") or {}).get("photos") or []:
        u = p.get("photoUrls") or {}
        url = u.get("fullScreenPhotoUrl") or u.get("nonFullScreenPhotoUrl")
        if url:
            photos.append({"href": url, "tags": [p["subdirectory"]] if p.get("subdirectory") else []})
    remarks = main.get("marketingRemarks") or []
    desc = next((r.get("marketingRemark") for r in remarks if isinstance(r, dict) and r.get("marketingRemark")), None)
    agents = main.get("listingAgents") or []
    ag = (agents[0].get("agentInfo") if agents and isinstance(agents[0], dict) else None) or {}
    buyer = main.get("buyingAgents") or []
    bag = (buyer[0].get("agentInfo") if buyer and isinstance(buyer[0], dict) else None) or {}
    land, impr = num(tax.get("taxableLandValue")), num(tax.get("taxableImprovementValue"))
    assessed = (land or 0) + (impr or 0) if (land or impr) else None
    features = _redfin_amenities(b)
    schools = []
    for s in (b.get("schoolsAndDistrictsInfo") or {}).get("servingThisHomeSchools") or []:
        schools.append({"name": s.get("name"), "rating": num(s.get("greatSchoolsRating")),
                        "distance_mi": num(s.get("distanceInMiles")), "levels": s.get("gradeRanges"),
                        "grades": s.get("gradeRanges"), "funding": s.get("institutionType"),
                        "district": s.get("districtName")})
    districts = (b.get("schoolsAndDistrictsInfo") or {}).get("districts") or []
    status = addr.get("status")
    d = blank_detail()
    d.update({
        "source_listing_id": str(a["listingId"]) if a.get("listingId") else None,
        "url": ("https://www.redfin.com" + addr["url"]) if isinstance(addr.get("url"), str) else None,
        "status": status.get("displayValue") if isinstance(status, dict) else status,
        "list_price": num((addr.get("priceInfo") or {}).get("amount")),
        "description": desc, "property_type": basic.get("propertyTypeName"),
        "beds": num(addr.get("beds")) if addr.get("beds") is not None else num(basic.get("beds")),
        "baths": num(addr.get("baths")) if addr.get("baths") is not None else num(basic.get("baths")),
        "baths_full": num(addr.get("numFullBaths")), "baths_half": num(addr.get("numPartialBaths")),
        "sqft": num(_v(addr.get("sqFt"))) or num(basic.get("totalSqFt")),
        "lot_sqft": num(_v(addr.get("lotSize"))) or num(basic.get("lotSqFt")),
        "year_built": _int(_v(addr.get("yearBuilt"))) or _int(basic.get("yearBuilt")),
        "stories": num(basic.get("numStories")),
        "hoa_fee": _money(_find(a, {"hoaDues"}) or _find(b, {"hoaDues"})),
        "tax_annual": num(tax.get("taxesDue")), "tax_year": _int(tax.get("rollYear")), "assessed_value": assessed,
        "lat": num(ll.get("latitude")), "lng": num(ll.get("longitude")), "apn": basic.get("apn"),
        "school_district": next((x.get("name") for x in districts if isinstance(x, dict) and x.get("name")), None),
        "mls_name": main.get("dataSourceName") or _find(a, {"listingSourceName"}),
        "mls_id": _int(a.get("dataSourceId")) and str(a.get("dataSourceId")),
        "mls_listing_id": _v(main.get("mlsId")) or _find(a, {"mlsId"}),
        "list_office": ag.get("brokerName") or main.get("listingBrokerName"),
        "list_agent_name": ag.get("agentName"), "list_agent_phone": ag.get("phoneNumber") or ag.get("agentPhone"),
        "list_agent_email": ag.get("agentEmail"), "list_agent_license": ag.get("agentLicenseNumber"),
        "buyer_agent_name": bag.get("agentName"), "buyer_office": bag.get("brokerName"),
        "photo_count": len(photos) or None, "photos": photos or None,
        "virtual_tours": [{"type": "virtual_tour", "href": t.get("url")} for t in
                          (a.get("mediaBrowserInfo") or {}).get("virtualTours") or [] if isinstance(t, dict) and t.get("url")] or None,
        "features": features or None,
        "agents": {"listing": agents, "buying": buyer} if (agents or buyer) else None,
        "extra": {"county": basic.get("countyName")},
        "price_history": [{"date": _ms_date(e.get("eventDate")), "event": e.get("eventDescription"),
                           "price": num(e.get("price")), "price_sqft": None, "source_name": e.get("source"),
                           "mls_listing_id": e.get("sourceId"), "status": e.get("mlsDescription")}
                          for e in (b.get("propertyHistoryInfo") or {}).get("events") or [] if isinstance(e, dict)],
        "tax_history": [{"year": _int(tax.get("rollYear")), "tax": num(tax.get("taxesDue")), "assessed_total": assessed,
                         "assessed_building": impr, "assessed_land": land, "market_total": None}] if tax else [],
        "schools": schools,
    })
    d.update(_feature_lists(features))
    return d


class RedfinDetail:
    name = "redfin"

    def __init__(self, client: SourceClient | None = None):
        self.client = client or detail_client("redfin")

    def fetch(self, source_id: str, source_listing_id: str | None = None) -> tuple[dict, dict]:
        raw, payloads = {}, {}
        for part, url in redfin_urls(source_id, source_listing_id).items():
            r = self.client.request("GET", url, headers=REDFIN_HEADERS)
            if r.status_code == 404:
                raise DetailError(f"redfin {part}: 404 for {source_id}")
            raw[part] = _json(r.text, f"redfin {part}")
            payloads[part] = _redfin_payload(raw[part], f"redfin {part}")
        return raw, normalize_redfin(payloads.get("aboveTheFold"), payloads.get("belowTheFold"))


FETCHERS = {"redfin": RedfinDetail, "realtor": RealtorDetail}
