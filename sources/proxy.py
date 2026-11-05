"""ProxyRotator (proxyrotator.com) gateway support: per-source opt-in, shared thread cap, rotate/retire, masking.

ProxyRotator's model is a *rotating gateway*: one host:port, Basic auth (username/password) or IP whitelist, and
the gateway hands out a new exit IP per request (or keeps one for up to 30 min on a sticky gateway). There is no
"fetch me a proxy" API to poll, so "rotation" here means: every request goes through the gateway, and after a
403/429/connection error we drop the pooled connection (fresh CONNECT -> new IP) and move to the next configured
gateway endpoint, retiring an endpoint for a cooldown after repeated failures.

Env (values only in .env, never logged):
  PROXYROTATOR_GATEWAY        host:port (comma-separated for several gateways, e.g. rotating + sticky ports)
  PROXYROTATOR_USERNAME       gateway username (omit both for IP-whitelist auth)
  PROXYROTATOR_PASSWORD       gateway password
  PROXYROTATOR_SCHEME         http (default) | socks5h
  PROXY_MAX_THREADS           in-flight cap for proxied traffic per process (default 5; alias PROXYROTATOR_MAX_THREADS)
  PROXY_BUDGET_GB             bandwidth budget per billing period, up+down, shared by all processes (default 150)
  PROXY_RESET_DATE            YYYY-MM-DD the counter resets (plan renewal; then monthly on that day). Default 2026-11-09
  PROXY_USAGE_FILE            persistent counter (default data/proxy_usage.json)
  PROXYROTATOR_DIRECT_FALLBACK  1 = when every endpoint is retired, go direct instead of waiting (default 0)
  <SOURCE>_USE_PROXYROTATOR   1/0 per source family (REDFIN default 1, everything else default 0)
  <SOURCE>_PROXY              explicit proxy URL for one source; wins over ProxyRotator
"""
from __future__ import annotations

import datetime as dt
import fcntl
import json
import os
import re
import threading
import time
from urllib.parse import quote

DEFAULT_ON = {"redfin"}
_CRED_RE = re.compile(r"(?P<scheme>[a-z0-9+.-]+://)(?P<cred>[^/@\s:]+(?::[^/@\s]*)?)@", re.I)


def mask(text: object) -> str:
    """Strip credentials from any proxy URL inside text (user:pass@ -> ***@), plus any literal secret values."""
    s = _CRED_RE.sub(lambda m: m.group("scheme") + "***@", str(text))
    for var in ("PROXYROTATOR_PASSWORD", "PROXYROTATOR_USERNAME"):
        v = os.environ.get(var)
        if v and len(v) >= 4:
            s = s.replace(v, "***").replace(quote(v, safe=""), "***")
    return s


def family(name: str) -> str:
    """'redfin_detail' -> 'redfin'."""
    return name.split("_", 1)[0].lower()


def enabled_for(name: str) -> bool:
    fam = family(name).upper()
    raw = os.environ.get(f"{name.upper()}_USE_PROXYROTATOR") or os.environ.get(f"{fam}_USE_PROXYROTATOR")
    if raw is None:
        return family(name) in DEFAULT_ON
    return raw.strip().lower() in ("1", "true", "yes", "on")


class ProxyBudgetExceeded(RuntimeError):
    """Bandwidth budget reached and direct fallback is off: the caller should skip this item."""


def _period_start(reset: dt.date, today: dt.date) -> dt.date:
    """Most recent monthly anniversary of `reset` that is <= today (before the first reset: the month before it)."""
    y, m = today.year, today.month
    for _ in range(14):
        try:
            d = dt.date(y, m, min(reset.day, 28) if reset.day > 28 else reset.day)
        except ValueError:
            d = dt.date(y, m, 28)
        if d <= today:
            return d
        y, m = (y, m - 1) if m > 1 else (y - 1, 12)
    return today


class UsageMeter:
    """Bytes through the proxy, persisted to a JSON file shared across processes (flock'd read-modify-write)."""

    def __init__(self, path: str, budget_gb: float, reset: dt.date, flush_bytes: int = 2_000_000,
                 flush_s: float = 30.0, today=dt.date.today, clock=time.monotonic):
        self.path, self.budget = path, int(budget_gb * 1e9)
        self.reset, self.today, self.clock = reset, today, clock
        self.flush_bytes, self.flush_s = flush_bytes, flush_s
        self._lock = threading.Lock()
        self._pending = 0
        self._last_flush = clock()
        self._total = self._sync(0)
        self._logged_gb = int(self._total // 1e9)

    def _sync(self, delta: int) -> int:
        period = _period_start(self.reset, self.today()).isoformat()
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(self.path, "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.seek(0)
            try:
                st = json.loads(f.read() or "{}")
            except ValueError:
                st = {}
            if st.get("period_start") != period:
                st = {"period_start": period, "bytes": 0, "requests": 0}
            st["bytes"] = int(st.get("bytes", 0)) + delta
            st["requests"] = int(st.get("requests", 0)) + (1 if delta else 0)
            st["updated"] = dt.datetime.now().isoformat(timespec="seconds")
            f.seek(0)
            f.truncate()
            f.write(json.dumps(st))
            return st["bytes"]

    def add(self, n: int) -> None:
        with self._lock:
            self._pending += n
            self._total += n
            if self._pending >= self.flush_bytes or self.clock() - self._last_flush >= self.flush_s:
                self.flush_locked()
            gb = int(self._total // 1e9)
            if gb > self._logged_gb:
                self._logged_gb = gb
                print(f"[proxy] bandwidth used this period: {self.used_gb:.2f} / {self.budget / 1e9:.0f} GB", flush=True)

    def flush_locked(self) -> None:
        self._total = self._sync(self._pending)
        self._pending = 0
        self._last_flush = self.clock()

    def flush(self) -> None:
        with self._lock:
            self.flush_locked()

    @property
    def used_gb(self) -> float:
        return self._total / 1e9

    def exhausted(self) -> bool:
        return self._total >= self.budget


class ProxyPool:
    def __init__(self, endpoints: list[str], max_threads: int = 5, retire_after: int = 3, cooldown: float = 120.0,
                 direct_fallback: bool = False, clock=time.monotonic, meter: UsageMeter | None = None):
        if not endpoints:
            raise ValueError("ProxyPool needs at least one endpoint")
        self.endpoints = endpoints
        self.sem = threading.BoundedSemaphore(max(1, max_threads))
        self.max_threads = max(1, max_threads)
        self.retire_after, self.cooldown = retire_after, cooldown
        self.direct_fallback = direct_fallback
        self.clock = clock
        self.meter = meter
        self._budget_logged = False
        self._lock = threading.Lock()
        self._i = 0
        self._fails = [0] * len(endpoints)
        self._retired_until = [0.0] * len(endpoints)
        self.generation = 0          # bumped on every rotation; clients drop their session when it changes
        self.stats = {"requests": 0, "ok": 0, "rotations": 0, "retired": 0, "direct": 0}

    def over_budget(self) -> bool:
        if not (self.meter and self.meter.exhausted()):
            return False
        if not self._budget_logged:
            self._budget_logged = True
            print(f"[proxy] bandwidth budget reached ({self.meter.used_gb:.2f} GB): proxied traffic stopped, "
                  f"{'falling back to direct' if self.direct_fallback else 'skipping proxied requests'}", flush=True)
        return True

    def record(self, nbytes: int) -> None:
        if self.meter:
            self.meter.add(nbytes)

    def current(self) -> tuple[int, str | None]:
        """(index, proxy URL) of the endpoint to use now; (-1, None) = go direct (fallback)."""
        if self.over_budget():
            if self.direct_fallback:
                with self._lock:
                    self.stats["direct"] += 1
                return -1, None
            raise ProxyBudgetExceeded("proxy bandwidth budget reached")
        with self._lock:
            now = self.clock()
            for k in range(len(self.endpoints)):
                i = (self._i + k) % len(self.endpoints)
                if self._retired_until[i] <= now:
                    self._i = i
                    self.stats["requests"] += 1
                    return i, self.endpoints[i]
            if self.direct_fallback:
                self.stats["direct"] += 1
                return -1, None
            # everything retired: use the one that comes back first rather than stalling
            i = min(range(len(self.endpoints)), key=lambda j: self._retired_until[j])
            self.stats["requests"] += 1
            return i, self.endpoints[i]

    def ok(self, i: int) -> None:
        with self._lock:
            self.stats["ok"] += 1
            if i >= 0:
                self._fails[i] = 0

    def fail(self, i: int) -> None:
        """403/429/connection error through endpoint i: rotate to a fresh IP (and the next endpoint)."""
        if i < 0:
            return
        with self._lock:
            self._fails[i] += 1
            self.stats["rotations"] += 1
            self.generation += 1
            if self._fails[i] >= self.retire_after:
                self._fails[i] = 0
                self._retired_until[i] = self.clock() + self.cooldown
                self.stats["retired"] += 1
            self._i = (i + 1) % len(self.endpoints)

    def summary(self) -> str:
        used = f" used_gb={self.meter.used_gb:.3f}/{self.meter.budget / 1e9:.0f}" if self.meter else ""
        return f"proxyrotator{used} endpoints={len(self.endpoints)} threads={self.max_threads} " + \
            " ".join(f"{k}={v}" for k, v in self.stats.items())


def _build_endpoints() -> list[str]:
    gw = os.environ.get("PROXYROTATOR_GATEWAY", "").strip()
    if not gw:
        return []
    scheme = os.environ.get("PROXYROTATOR_SCHEME", "http").strip() or "http"
    user, pw = os.environ.get("PROXYROTATOR_USERNAME", ""), os.environ.get("PROXYROTATOR_PASSWORD", "")
    auth = f"{quote(user, safe='')}:{quote(pw, safe='')}@" if user else ""
    out = []
    for hp in (x.strip() for x in gw.split(",")):
        if hp:
            hp = hp.split("://", 1)[-1]
            out.append(f"{scheme}://{auth}{hp}")
    return out


_POOL: ProxyPool | None = None
_POOL_LOCK = threading.Lock()


def pool_from_env() -> ProxyPool | None:
    """Process-wide pool (one thread cap for the whole plan), or None when no gateway is configured."""
    global _POOL
    with _POOL_LOCK:
        if _POOL is None:
            eps = _build_endpoints()
            if eps:
                here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                meter = UsageMeter(os.environ.get("PROXY_USAGE_FILE") or os.path.join(here, "data", "proxy_usage.json"),
                                   float(os.environ.get("PROXY_BUDGET_GB", 150) or 150),
                                   dt.date.fromisoformat(os.environ.get("PROXY_RESET_DATE") or "2026-11-09"))
                threads = os.environ.get("PROXY_MAX_THREADS") or os.environ.get("PROXYROTATOR_MAX_THREADS") or 5
                _POOL = ProxyPool(eps, max_threads=int(threads), meter=meter,
                                  direct_fallback=os.environ.get("PROXYROTATOR_DIRECT_FALLBACK", "0") == "1")
            if _POOL is not None:
                import atexit
                atexit.register(lambda p=_POOL: (p.meter and p.meter.flush(), print(f"[proxy] {p.summary()}", flush=True)))
        return _POOL
