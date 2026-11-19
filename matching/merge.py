"""Merge per-source silver records into one canonical listing per (property_key, listing_type)."""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Iterable

from matching.address import normalize_address, property_key, street_core, zip5
from normalization.schema import APP_COLS

DEFAULT_ORDER = ["realtor", "redfin", "homes"]
# per-field precedence overrides (first source with a non-null value wins)
FIELD_ORDER = {
    "lat": ["redfin", "realtor", "homes"],
    "lng": ["redfin", "realtor", "homes"],
    "sold_date": ["redfin", "realtor", "homes"],
    "sqft": ["redfin", "realtor", "homes"],
    "status": ["redfin", "realtor", "homes"],  # raw MLS status where available
}
# sold price: Redfin reports a value in non-disclosure states where Realtor is empty
TYPE_FIELD_ORDER = {("sold", "price"): ["redfin", "realtor", "homes"]}
TRACKED = ["price", "list_price", "status", "beds", "baths", "sqft", "lot_sqft", "year_built", "sold_date",
           "listed_date", "available_from", "deposit", "property_type"]  # photo_url: CDN urls rotate, not a change
CANON_FIELDS = [c for c in APP_COLS if c not in ("external_id",)]


def fallback_key(rec: dict) -> str | None:
    """Identity for a record with no house number and no coordinates (land lots like "Water Oak Ln Lot 48"),
    which property_key() cannot key. The prefixes never collide with a proper key ("<zip>|..." / "geo|...").

    addr|<zip>|<street>|<unit>  street + lot/unit designator + ZIP: the same lot from two sources still merges
    src|<source>|<source_id>    nothing in the address tells two lots apart: one listing per source record
    """
    street, unit = normalize_address(rec.get("address"))
    z = zip5(rec.get("zip"))
    if street and unit and z:
        return f"addr|{z}|{street_core('0 ' + street)[2:]}|{unit}"
    if rec.get("source") and rec.get("source_id") not in (None, ""):
        return f"src|{rec['source']}|{rec['source_id']}"
    return None


def key_for(rec: dict) -> str | None:
    return (property_key(rec.get("address"), rec.get("zip"), lat=rec.get("lat"), lng=rec.get("lng"))
            or fallback_key(rec))


def _lot_form(rec: dict) -> tuple | None:
    """(zip, street, unit) of a no-house-number address, to pair a fallback-keyed record with a geo-keyed one."""
    street, unit = normalize_address(rec.get("address"))
    z = zip5(rec.get("zip"))
    if not street or not unit or not z or street[0].isdigit():
        return None
    return z, street_core("0 " + street)[2:], unit


def canonical_id(pkey: str) -> str:
    return "la:" + hashlib.sha1(pkey.encode()).hexdigest()[:16]


def row_hash(rec: dict, fields: Iterable[str] = TRACKED) -> str:
    return hashlib.sha1(json.dumps([rec.get(f) for f in fields], default=str).encode()).hexdigest()


def _rank(order: list[str], src: str) -> int:
    return order.index(src) if src in order else len(order)


def merge_group(pkey: str, listing_type: str, recs: list[dict]) -> dict:
    """recs: silver records (same property & type) from one or more sources."""
    out = {"property_key": pkey, "listing_type": listing_type, "external_id": canonical_id(pkey)}
    # deterministic: several records of one source can share a key (builder plans, duplicate feeds)
    by_src = sorted(recs, key=lambda r: (_rank(DEFAULT_ORDER, r["source"]), str(r.get("source_id"))))
    for f in CANON_FIELDS:
        if f == "listing_type":
            continue
        order = TYPE_FIELD_ORDER.get((listing_type, f)) or FIELD_ORDER.get(f) or DEFAULT_ORDER
        val = None
        for r in sorted(by_src, key=lambda r: (_rank(order, r["source"]), str(r.get("source_id")))):
            if r.get(f) not in (None, ""):
                val = r[f]
                break
        out[f] = val
    street, unit = normalize_address(out.get("address"))
    out["unit"] = unit or None
    out["county_fips"] = next((r.get("county_fips") for r in by_src if r.get("county_fips")), None)
    out["sources"] = sorted({r["source"] for r in recs}, key=lambda s: _rank(DEFAULT_ORDER, s))
    out["source_ids"] = {r["source"]: r["source_id"] for r in reversed(by_src)}
    dup = {s: n for s in out["sources"] if (n := sum(1 for r in recs if r["source"] == s)) > 1}
    if dup:
        out["duplicates"] = dup
    out["urls"] = {r["source"]: r.get("url") for r in reversed(by_src) if r.get("url")}
    out["extra"] = {r["source"]: r.get("extra") or {} for r in reversed(by_src)}
    out["row_hash"] = row_hash(out)
    return out


def merge(records: Iterable[dict]) -> tuple[list[dict], list[dict]]:
    """Returns (canonical listings, unmatched records without any usable key)."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    nokey, keyed = [], []
    geo: dict[tuple, set] = defaultdict(set)  # (type, zip, street, unit) -> geo keys of lots that have coordinates
    for r in records:
        # always re-key with the current rules (a stored key may come from older matching code)
        k = key_for(r) or r.get("property_key")
        if not k:
            nokey.append(r)
            continue
        keyed.append((k, r))
        if k.startswith("geo|") and (lf := _lot_form(r)):
            geo[(r["listing_type"], *lf)].add(k)
    for k, r in keyed:
        if k.startswith("addr|"):
            # the same lot seen with coordinates by another source: join that listing instead of duplicating it
            # (only when exactly one such lot exists; ambiguous ones keep their own key)
            hit = geo.get((r["listing_type"], *k.split("|")[1:]))
            if hit and len(hit) == 1:
                k = next(iter(hit))
        r["property_key"] = k
        groups[(k, r["listing_type"])].append(r)
    join_near(groups)
    return [merge_group(k, t, rs) for (k, t), rs in groups.items()], nokey


def _dist_m(a: dict, b: dict) -> float | None:
    import math
    try:
        la1, lo1, la2, lo2 = float(a["lat"]), float(a["lng"]), float(b["lat"]), float(b["lng"])
    except (KeyError, TypeError, ValueError):
        return None
    x = math.radians(lo2 - lo1) * math.cos(math.radians((la1 + la2) / 2))
    return 6371000 * math.hypot(x, math.radians(la2 - la1))


def _close(a, b, tol: float) -> bool:
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return True  # unknown on one side: no evidence against
    return abs(a - b) <= tol * max(abs(a), abs(b), 1)


def _same_home(a: dict, b: dict) -> bool:
    d = _dist_m(a, b)
    if d is None or d > NEAR_M:
        return False
    try:
        if a.get("beds") is not None and b.get("beds") is not None and abs(float(a["beds"]) - float(b["beds"])) > 1:
            return False
    except (TypeError, ValueError):
        pass
    return _close(a.get("sqft"), b.get("sqft"), 0.15) and _close(a.get("price"), b.get("price"), 0.2)


NEAR_M = 80  # two sources' pins of one home are almost always within this


def join_near(groups: dict) -> None:
    """Realtor+Redfin copies of one home whose street text differs ("Mill Creek Rd" vs "Mill Creek") get different
    keys. Join a single-source group to the one other group with the same ZIP, house number, first street word and unit, from the other
    source(s), within NEAR_M metres and with similar beds / sqft / price. Ambiguous cases (several candidates) stay
    apart. The joined listing keeps the smaller key, so the choice is the same every run."""
    idx: dict[tuple, list] = defaultdict(list)
    for (k, t), rs in groups.items():
        parts = k.split("|")
        if len(parts) == 3 and parts[0].isdigit() and parts[1]:
            w = parts[1].split()
            # ZIP, house number, first street word, unit: "100 mill creek" and "100 mill" meet, "100 oak" does not
            idx[(t, parts[0], w[0], w[1] if len(w) > 1 else "", parts[2])].append((k, t))  # w[0] = house number
    for cands in idx.values():
        if len(cands) < 2:
            continue
        for a in sorted(cands):
            if a not in groups:
                continue
            srcs_a = {r["source"] for r in groups[a]}
            if len(srcs_a) != 1:
                continue
            ra = groups[a][0]
            hits = [b for b in cands if b != a and b in groups and not (srcs_a & {r["source"] for r in groups[b]})
                    and _same_home(ra, groups[b][0])]
            if len(hits) != 1:
                continue
            b = hits[0]
            keep, drop = (a, b) if a < b else (b, a)
            for r in groups[drop]:
                r["property_key"] = keep[0]
            groups[keep].extend(groups.pop(drop))


def merge_stats(listings: list[dict]) -> dict:
    multi = sum(1 for l in listings if len(l["sources"]) > 1)
    per = defaultdict(int)
    for l in listings:
        per["+".join(l["sources"])] += 1
    return {"listings": len(listings), "multi_source": multi, "by_sources": dict(per)}
