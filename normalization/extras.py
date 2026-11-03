"""Source-specific fields the app COLS don't hold (see docs/field_inventory.md); kept in JSONB `extra`."""
from __future__ import annotations


def _get(d, path):
    for k in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    if isinstance(d, dict) and set(d) <= {"value", "level", "label"}:
        d = d.get("value")
    return d


def pick(raw: dict, mapping: dict[str, str]) -> dict:
    out = {}
    for name, path in mapping.items():
        v = _get(raw, path)
        if v not in (None, "", [], {}):
            out[name] = v
    return out


REDFIN_GIS = {
    "mls_id": "mlsId", "mls_status": "mlsStatus", "listing_id": "listingId", "data_source_id": "dataSourceId",
    "dom": "dom", "time_on_redfin_ms": "timeOnRedfin", "hoa": "hoa", "price_per_sqft": "pricePerSqFt",
    "full_baths": "fullBaths", "partial_baths": "partialBaths", "stories": "stories",
    "garage_spaces": "skGarageSpaces", "parking_spaces": "skParkingSpaces", "pool_type": "skPoolType",
    "neighborhood": "location", "is_hot": "isHot", "is_new_construction": "isNewConstruction",
    "listing_tags": "listingTags", "key_facts": "keyFacts", "has_virtual_tour": "hasVirtualTour",
    "has_3d_tour": "has3DTour", "search_status": "searchStatus", "time_zone": "timeZone",
}
REDFIN_RENT = {
    "rental_id": "rentalExtension.rentalId", "property_name": "rentalExtension.propertyName",
    "units_available": "rentalExtension.numAvailableUnits", "rent_max": "rentalExtension.rentPriceRange.max",
    "beds_max": "rentalExtension.bedRange.max", "baths_max": "rentalExtension.bathRange.max",
    "sqft_max": "rentalExtension.sqftRange.max", "feed_source": "rentalExtension.feedSource",
    "last_updated": "rentalExtension.lastUpdated", "income_restricted": "rentalExtension.isIncomeRestricted",
    "senior_living": "rentalExtension.isSeniorLiving", "student": "rentalExtension.isStudent",
    "phone": "rentalExtension.desktopPhone",
}
REALTOR = {
    "mls_id": "source.listing_id", "mls_name": "source.name", "mls_raw_status": "source.raw.status",
    "listing_id": "listing_id", "county_fips": "location.county.fips_code", "county": "location.county.name",
    "hoa_fee": "hoa.fee", "price_per_sqft": "price_per_sqft", "baths_full": "description.baths_full",
    "baths_half": "description.baths_half", "stories": "description.stories", "garage": "description.garage",
    "beds_max": "description.beds_max", "sqft_max": "description.sqft_max", "list_price_max": "list_price_max",
    "price_reduced_amount": "price_reduced_amount", "last_price_change_amount": "last_price_change_amount",
    "last_price_change_date": "last_price_change_date", "last_status_change_date": "last_status_change_date",
    "last_update_date": "last_update_date", "estimate": "estimate.estimate", "estimate_high": "estimate.estimate_high",
    "estimate_low": "estimate.estimate_low", "tax_record_id": "tax_record.public_record_id",
    "new_construction": "flags.is_new_construction", "price_reduced": "flags.is_price_reduced",
    "pet_policy": "pet_policy.text", "tags": "tags", "community": "community.description.name",
    "photo_count": "photo_count"
}


def realtor_extra(raw: dict) -> dict:
    out = pick(raw, REALTOR)
    agents = [a.get("name") for a in raw.get("advertisers") or [] if isinstance(a, dict) and a.get("name")]
    offices = [(a.get("office") or {}).get("name") for a in raw.get("advertisers") or [] if isinstance(a, dict)]
    if agents:
        out["agents"] = agents
    if any(offices):
        out["offices"] = [o for o in offices if o]
    return out
