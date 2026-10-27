"""Discover the widest field set Realtor's home_search accepts.

Introspection is disabled on the endpoint, so we start from a large candidate
selection, send it, and drop every field the server rejects ("Cannot query
field"), repeating until the query validates. The result is written to
sources/realtor_fields.graphql and used by sources/realtor.py.

usage: python -m tools.realtor_fields
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from sources.http import SourceClient  # noqa: E402
from sources.realtor import ENDPOINT, HEADERS  # noqa: E402

PHONES = {"number": 1, "type": 1, "ext": 1, "primary": 1, "trackable": 1}
ADDRESS = {"line": 1, "street_number": 1, "street_name": 1, "street_suffix": 1, "street_direction": 1,
           "street_post_direction": 1, "unit": 1, "city": 1, "state_code": 1, "state": 1, "postal_code": 1,
           "country": 1, "validation_code": 1, "coordinate": {"lat": 1, "lon": 1, "accuracy": 1}}
OFFICE = {"fulfillment_id": 1, "name": 1, "email": 1, "href": 1, "slogan": 1, "photo": {"href": 1},
          "phones": PHONES, "address": ADDRESS, "mls_set": 1, "hours": 1, "website": 1}
DESCRIPTION = {k: 1 for k in (
    "type sub_type name beds beds_min beds_max baths baths_min baths_max baths_full baths_half baths_3qtr "
    "baths_1qtr baths_consolidated baths_full_calc baths_partial_calc sqft sqft_min sqft_max lot_sqft year_built "
    "year_renovated garage garage_min garage_max garage_type stories text sold_price sold_date pool heating cooling "
    "units rooms construction exterior_construction fireplace logo styles zoning".split())}
CANDIDATES = {
    **{k: 1 for k in (
        "property_id listing_id plan_id status list_price list_price_min list_price_max price_reduced_amount "
        "list_date last_update_date last_status_change_date last_sold_date last_sold_price last_price_change_date "
        "last_price_change_amount href permalink photo_count matterport tags lead_attributes "
        "days_on_market price_per_sqft listing_date_type community_id search_promotions "
        "mortgage_estimate".split())},
    "primary_photo": {"href": 1}, "photos": {"href": 1, "type": 1, "title": 1, "description": 1, "tags": {"label": 1, "probability": 1}},
    "virtual_tours": {"href": 1, "type": 1, "category": 1},
    "videos": {"href": 1}, "home_tours": {"virtual_tours": {"href": 1, "type": 1}},
    "open_houses": {"start_date": 1, "end_date": 1, "description": 1, "time_zone": 1, "dst": 1, "methods": 1, "href": 1},
    "pet_policy": {"cats": 1, "dogs": 1, "dogs_small": 1, "dogs_large": 1, "text": 1},
    "hoa": {"fee": 1}, "estimate": {"estimate": 1, "estimate_high": 1, "estimate_low": 1, "date": 1},
    "current_estimates": {"estimate": 1, "estimate_high": 1, "estimate_low": 1, "date": 1, "isbest_homevalue": 1,
                          "source": {"name": 1, "type": 1}},
    "tax_record": {"public_record_id": 1, "cl_id": 1, "apn": 1},
    "details": {"category": 1, "text": 1, "parent_category": 1},
    "description": DESCRIPTION,
    "location": {"address": ADDRESS, "county": {"name": 1, "fips_code": 1, "state_code": 1},
                 "neighborhoods": {"name": 1, "city": 1, "level": 1, "id": 1, "geo_type": 1, "slug_id": 1},
                 "street_view_url": 1, "street_view_metadata_url": 1, "search_areas": {"city": 1, "state_code": 1}},
    "flags": {k: 1 for k in (
        "is_new_listing is_pending is_contingent is_foreclosure is_price_reduced is_new_construction is_coming_soon "
        "is_plan is_subdivision is_short_sale is_senior_community is_for_rent is_garage_present is_deal_available "
        "is_usda_eligible is_price_excluded is_installment is_chat_enabled is_premium_listing is_showcase "
        "is_rental_community is_virtual_tour_available is_sales_builder is_cats_allowed is_dogs_allowed".split())},
    "units": {"unit_id": 1, "list_price": 1, "list_price_min": 1, "list_price_max": 1, "availability": {"date": 1},
              "description": {"name": 1, "beds": 1, "baths": 1, "baths_full": 1, "baths_half": 1, "sqft": 1,
                              "sqft_min": 1, "sqft_max": 1},
              "photos": {"href": 1}},
    "community": {"property_id": 1, "description": {"name": 1}, "permalink": 1},
    "advertisers": {"fulfillment_id": 1, "name": 1, "type": 1, "email": 1, "href": 1, "slogan": 1, "state_license": 1,
                    "nrds_id": 1, "builder": {"fulfillment_id": 1, "name": 1, "href": 1}, "photo": {"href": 1},
                    "phones": PHONES, "office": OFFICE, "broker": {"fulfillment_id": 1, "name": 1}},
    "source": {"id": 1, "type": 1, "name": 1, "listing_id": 1, "plan_id": 1, "spec_id": 1, "community_id": 1,
               "disclaimer": {"text": 1, "href": 1, "logo": {"href": 1}}, "raw": {"status": 1, "style": 1, "tax_amount": 1},
               "agents": {"agent_id": 1, "agent_name": 1, "office_id": 1, "office_name": 1, "office_phone": 1, "type": 1, "id": 1}},
    "branding": {"name": 1, "phone": 1, "type": 1, "photo": 1, "link": 1, "accent_color": 1},
    "products": {"brand_name": 1, "products": 1},
    "other_listings": {"rdc": {"listing_id": 1, "listing_key": 1, "status": 1, "primary": 1}},
    "buyers": {"fulfillment_id": 1, "name": 1, "type": 1},
    "consumer_advertisers": {"advertiser_id": 1, "office_id": 1, "agent_id": 1, "name": 1, "phone": 1, "type": 1, "href": 1, "photo": {"href": 1}, "show_realtor_logo": 1},
}


def render(sel: dict) -> str:
    return " ".join(k if v == 1 else f"{k}{{{render(v)}}}" for k, v in sel.items())


def render_lines(sel: dict, path=(), lines=None):
    """One field per line so validation error locations map back to a field path."""
    lines = [] if lines is None else lines
    for k, v in sel.items():
        if v == 1:
            lines.append((k, path + (k,)))
        else:
            lines.append((k + "{", path + (k,)))
            render_lines(v, path + (k,), lines)
            lines.append(("}", None))
    return lines


def drop(sel: dict, path: tuple) -> None:
    node = sel
    for k in path[:-1]:
        node = node[k]
    node.pop(path[-1], None)
    # prune empty parents
    for i in range(len(path) - 1, 0, -1):
        node = sel
        for k in path[:i - 1]:
            node = node[k]
        if node.get(path[i - 1]) == {}:
            node.pop(path[i - 1])


def main() -> None:
    client = SourceClient("realtor", min_interval=2.0)
    sel = json.loads(json.dumps(CANDIDATES))
    errs = []
    for _ in range(60):
        lines = render_lines(sel)
        head = ("query ConsumerSearchQuery($query:HomeSearchCriteria!,$limit:Int){home_search(query:$query,limit:$limit)"
                "{total results{")
        q = head + "\n" + "\n".join(t for t, _ in lines) + "\n}}}"
        body = {"operationName": "ConsumerSearchQuery", "query": q,
                "variables": {"query": {"status": ["for_sale"], "search_location": {"location": "Travis County, TX"}}, "limit": 3}}
        data = client.request_json("POST", ENDPOINT, headers=HEADERS, json=body) or {}
        errs = data.get("errors") or []
        bad = set()
        for e in errs:
            for loc in e.get("locations") or []:
                idx = loc["line"] - 2
                if 0 <= idx < len(lines) and lines[idx][1]:
                    bad.add(lines[idx][1])
        if not bad:
            if errs:
                print("unhandled errors:", errs[:3])
            break
        print("dropping", sorted(".".join(b) for b in bad))
        for b in sorted(bad, key=len, reverse=True):
            try:
                drop(sel, b)
            except KeyError:
                pass
    out = ROOT / "sources" / "realtor_fields.graphql"
    out.write_text(render(sel) + "\n")
    print(f"wrote {out} ({len(render(sel))} chars); errors={bool(errs)}")


if __name__ == "__main__":
    main()
