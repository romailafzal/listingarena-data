"""Pure change detection between current DB state and a freshly merged snapshot."""
from __future__ import annotations

from matching.merge import TRACKED


def diff(before: dict, after: dict, fields=TRACKED) -> dict:
    return {f: {"before": before.get(f), "after": after.get(f)} for f in fields
            if _norm(before.get(f)) != _norm(after.get(f))}


def _norm(v):
    from decimal import Decimal
    if isinstance(v, Decimal):
        v = float(v)
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if v is not None and not isinstance(v, (int, float, str, bool)):
        return str(v)
    return v


def detect(current: dict[str, dict], incoming: dict[str, dict], removable: set[str] | None = None) -> list[dict]:
    """current/incoming: {property_key: listing}. current listings carry `active`.

    removable: property keys whose disappearance may count as removal (all of the listing's sources
    completed their county fetch). None = everything removable.
    Returns events: {event, property_key, changes}.
    """
    events = []
    for k, new in incoming.items():
        old = current.get(k)
        if old is None:
            events.append({"event": "new", "property_key": k, "changes": {}})
        elif not old.get("active", True):
            events.append({"event": "relisted", "property_key": k, "changes": diff(old, new)})
        elif old.get("row_hash") != new.get("row_hash"):
            d = diff(old, new)
            if d:
                events.append({"event": "changed", "property_key": k, "changes": d})
    for k, old in current.items():
        if k not in incoming and old.get("active", True) and (removable is None or k in removable):
            events.append({"event": "removed", "property_key": k, "changes": {}})
    return events
