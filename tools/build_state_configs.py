"""Build states/<st>.json (county name, FIPS, redfin_region_id, realtor_location) for 50 states + DC.

Counties and FIPS come from the Census 2020 county Gazetteer (downloaded). Redfin region ids are not
derivable from public data without probing Redfin county by county, so they are seeded from a reference
JSON set (--seed DIR, files named <state_name>.json with counties[{name, redfin_region_id,
realtor_location, redfin_rental_polygon?}]) matched by county name. Counties absent from the seed get
realtor_location "<County>, <ST>" and redfin_region_id null. --verify-realtor checks unmatched
locations live (slow; polite pacing).

usage: python -m tools.build_state_configs --seed /path/to/states_dir [--gazetteer file.txt]
"""
from __future__ import annotations

import argparse
import io
import json
import re
import sys
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GAZETTEER_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_counties_national.zip"
STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado",
    "CT": "Connecticut", "DE": "Delaware", "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan",
    "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
    "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}


def key(name: str) -> str:
    n = name.lower().replace("saint ", "st. ").replace("ste. ", "sainte ")
    n = re.sub(r"\b(county|parish|borough|census area|city and borough|municipality|municipio)\b", "", n)
    return re.sub(r"[^a-z0-9]", "", n)


def load_gazetteer(path: str | None) -> list[dict]:
    if path:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
    else:
        raw = urllib.request.urlopen(GAZETTEER_URL, timeout=120).read()
        z = zipfile.ZipFile(io.BytesIO(raw))
        text = z.read(z.namelist()[0]).decode("utf-8", errors="replace")
    rows = []
    lines = text.splitlines()
    header = [h.strip() for h in lines[0].split("\t")]
    for line in lines[1:]:
        cells = [c.strip() for c in line.split("\t")]
        if len(cells) >= len(header):
            rows.append(dict(zip(header, cells)))
    return rows


def county_type(name: str) -> str:
    return "independent_city" if name.lower().endswith(" city") and "county" not in name.lower() else "county"


def build(gaz: list[dict], seed_dir: Path | None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for st, sname in STATE_NAMES.items():
        seed = {}
        if seed_dir:
            f = seed_dir / (sname.lower().replace(" ", "_") + ".json")
            if f.exists():
                for c in json.loads(f.read_text())["counties"]:
                    seed[(key(c["name"]), c.get("type", "county"))] = c
                    seed.setdefault((key(c["name"]), "any"), c)
        counties = []
        for row in sorted((r for r in gaz if r["USPS"] == st), key=lambda r: r["GEOID"]):
            name, ctype = row["NAME"], county_type(row["NAME"])
            s = seed.get((key(name), ctype)) or seed.get((key(name), "any")) or {}
            c = {"name": name, "fips": row["GEOID"], "type": ctype,
                 "redfin_region_id": s.get("redfin_region_id"),
                 "realtor_location": s.get("realtor_location") or f"{name}, {st}",
                 "lat": float(row["INTPTLAT"]), "lng": float(row["INTPTLONG"])}
            if s.get("redfin_rental_polygon"):
                c["redfin_rental_polygon"] = s["redfin_rental_polygon"]
            counties.append(c)
        out[st] = {"state": st, "name": sname, "counties": counties}
    return out


def main(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", help="dir of reference <state_name>.json files with redfin_region_id")
    ap.add_argument("--gazetteer", help="local Census gazetteer .txt (default: download)")
    ap.add_argument("--out", default=str(ROOT / "states"))
    a = ap.parse_args(argv)
    data = build(load_gazetteer(a.gazetteer), Path(a.seed) if a.seed else None)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    missing = 0
    for st, cfg in data.items():
        (out / f"{st.lower()}.json").write_text(json.dumps(cfg, indent=1) + "\n")
        m = [c["name"] for c in cfg["counties"] if not c["redfin_region_id"]]
        missing += len(m)
        if m:
            print(f"{st}: {len(m)} without redfin_region_id: {', '.join(m[:6])}{'...' if len(m) > 6 else ''}")
    print(f"wrote {len(data)} files, {sum(len(c['counties']) for c in data.values())} counties, {missing} without redfin id")


if __name__ == "__main__":
    sys.exit(main())
