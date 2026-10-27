"""Address normalization -> property_key used to match the same home across sources.

key = "<zip5>|<number> <predir> <street> <suffix> <postdir>|<unit>" all lowercase, abbreviations per USPS.
"""
from __future__ import annotations

import re

DIRECTIONS = {"north": "n", "south": "s", "east": "e", "west": "w", "northeast": "ne", "northwest": "nw",
              "southeast": "se", "southwest": "sw", "n": "n", "s": "s", "e": "e", "w": "w",
              "ne": "ne", "nw": "nw", "se": "se", "sw": "sw"}
SUFFIXES = {
    "street": "st", "st": "st", "str": "st", "avenue": "ave", "ave": "ave", "av": "ave", "avn": "ave",
    "road": "rd", "rd": "rd", "drive": "dr", "dr": "dr", "drv": "dr", "lane": "ln", "ln": "ln",
    "boulevard": "blvd", "blvd": "blvd", "boul": "blvd", "court": "ct", "ct": "ct", "circle": "cir",
    "cir": "cir", "circ": "cir", "place": "pl", "pl": "pl", "terrace": "ter", "ter": "ter", "trail": "trl",
    "trl": "trl", "parkway": "pkwy", "pkwy": "pkwy", "pkway": "pkwy", "highway": "hwy", "hwy": "hwy",
    "way": "way", "wy": "way", "cove": "cv", "cv": "cv", "loop": "loop", "lp": "loop", "square": "sq",
    "sq": "sq", "path": "path", "pass": "pass", "run": "run", "point": "pt", "pt": "pt", "crossing": "xing",
    "xing": "xing", "bend": "bnd", "bnd": "bnd", "hollow": "holw", "holw": "holw", "creek": "crk", "crk": "crk",
    "ridge": "rdg", "rdg": "rdg", "view": "vw", "vw": "vw", "glen": "gln", "gln": "gln", "heights": "hts",
    "hts": "hts", "hill": "hl", "hl": "hl", "valley": "vly", "vly": "vly", "expressway": "expy", "expy": "expy",
    "freeway": "fwy", "fwy": "fwy", "plaza": "plz", "plz": "plz", "row": "row", "walk": "walk", "alley": "aly",
    "aly": "aly", "trace": "trce", "trce": "trce", "meadow": "mdw", "meadows": "mdws", "park": "park",
    "farm to market": "fm", "ranch road": "rr", "county road": "cr",
}
# words that introduce a unit designator; all collapse to the bare unit id
UNIT_WORDS = r"(?:apt|apartment|unit|ste|suite|bldg|building|fl|floor|rm|room|lot|spc|space|trlr|ph|#|no\.?)"
UNIT_RE = re.compile(rf"(?:\s|,|^){UNIT_WORDS}\s*#?\s*([a-z0-9-]+)\s*$")
HASH_RE = re.compile(r"\s*#\s*([a-z0-9-]+)\s*$")
# route names: (pattern, replacement) applied in order; all highway spellings collapse to "hwy N"
ROUTES = [
    (r"\bfarm to market( road| rd)?\b", "fm"), (r"\branch to market( road| rd)?\b", "rm"), (r"\branch road\b", "rr"),
    (r"\bcounty road\b", "cr"), (r"\bfm (?:road|rd) #? ?(\d+)", r"fm \1"), (r"\b(fm|rm|rr|cr) (\d+) (?:road|rd)\b", r"\1 \2"),
    (r"\b(?:state highway|state hwy|us highway|us hwy|highway|hwy|tx|sh|us|state route|sr|route|rte)[ -](\d+[a-z]?)\b", r"hwy \1"),
    (r"\b(?:interstate|ih|i)[ -]?(\d+)\b", r"i \1"),
]
MULTI = {}


def zip5(z) -> str:
    m = re.match(r"\s*(\d{5})", str(z or ""))
    return m.group(1) if m else ""


def split_unit(line: str) -> tuple[str, str]:
    s = line.lower().strip().rstrip(",")
    for rx in (UNIT_RE, HASH_RE):
        m = rx.search(s)
        if m and m.start() > 0:
            if re.search(r"\b(fm|rm|rr|cr|hwy|highway|road|rd)$", s[:m.start()].strip(" ,")) and m.group(1).isdigit():
                return s.replace("#", " "), ""  # "Farm to Market Rd # 2429" is a route number, not a unit
            unit = m.group(1).lstrip("0") or "0"
            return s[:m.start()].strip(" ,"), unit
    return s, ""


def normalize_street(street: str) -> str:
    s = street.lower().replace(".", " ").replace(",", " ")
    s = re.sub(r"[^a-z0-9/ -]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for rx, rep in ROUTES:
        s = re.sub(rx, rep, s)
    toks = s.split()
    out = []
    for i, t in enumerate(toks):
        if t in DIRECTIONS and len(toks) > 2 and (i == 1 or i == len(toks) - 1):
            out.append(DIRECTIONS[t])
        elif t in SUFFIXES and i == len(toks) - 1 or (t in SUFFIXES and i == len(toks) - 2 and toks[-1] in DIRECTIONS):
            out.append(SUFFIXES[t])
        else:
            out.append(re.sub(r"^0+(\d)", r"\1", t) if i == 0 else t)
    return " ".join(out)


def normalize_address(line: str | None, unit: str | None = None) -> tuple[str, str]:
    if not line:
        return "", ""
    street, u = split_unit(line)
    if unit:
        u = str(unit).lower().replace("#", "").replace("unit", "").replace("apt", "").strip().lstrip("0") or u
    return normalize_street(street), u


def street_core(street: str) -> str:
    """Matching form: drop directionals and the trailing suffix(es) so '805 S Oak St' == '805 Oak St S' == '805 S Oak'."""
    toks = street.split()
    if not toks:
        return ""
    num, rest = toks[0], [t for t in toks[1:] if t not in DIRECTIONS]
    while len(rest) > 1 and rest[-1] in SUFFIXES.values() and rest[-1] != "hwy":
        rest.pop()
    while len(rest) > 1 and rest[-1] in SUFFIXES and rest[-2] not in ("fm", "rm", "rr", "cr"):
        rest.pop()  # "church lane ln"
    return " ".join([num] + rest)


def property_key(address: str | None, zip_code=None, unit: str | None = None, lat=None, lng=None) -> str | None:
    street, u = normalize_address(address, unit)
    z = zip5(zip_code)
    if street and z and re.match(r"\d", street):
        return f"{z}|{street_core(street)}|{u}"
    if lat is not None and lng is not None:  # no street number: fall back to rounded coordinates (~11 m)
        return f"geo|{round(float(lat), 4)}|{round(float(lng), 4)}|{u}"
    return None
