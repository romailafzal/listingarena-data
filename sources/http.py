"""Shared HTTP client: per-source pacing + in-flight cap, retry with backoff, block detection, proxies, rotating headers.

Concurrency knobs (env, per source; NAME = REDFIN / REALTOR / ...):
  NAME_MIN_INTERVAL  global spacing between request *starts* (s). 0 = no pacing. Slots are reserved under a
                     lock but the wait happens outside it, so with 0 nothing is serialized.
  NAME_MAX_INFLIGHT  cap on simultaneous HTTP calls for this client (adaptive, see AdaptiveLimiter). Unset = no cap.
  NAME_TIMEOUT       read timeout per request (s, default 30; connect timeout is 10). A request that times out is
                     retried with backoff and counts as an error for the adaptive cap.
  NAME_PAGE_PARALLEL parallel page fetches inside one county/type search (read by the sources via
                     SourceClient.page_parallel). Default 1 = sequential pagination.
Detail/per-home fetchers should create their OWN SourceClient (e.g. SourceClient("redfin_detail", ...)) so they
get independent, lower limits; the search-level concurrency above must not be applied to detail calls.
"""
from __future__ import annotations

import json
import os
import random
import threading
import time
from typing import Any

import requests

from sources.proxy import ProxyBudgetExceeded, mask, mode_for, pool_from_env

RETRY_STATUS = {403, 429, 500, 502, 503, 504}

# Realistic desktop browser identities; one is picked per request.
BROWSERS = [
    {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
     "sec-ch-ua": '"Google Chrome";v="141", "Chromium";v="141", "Not(A:Brand";v="24"', "sec-ch-ua-platform": '"macOS"'},
    {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
     "sec-ch-ua": '"Google Chrome";v="141", "Chromium";v="141", "Not(A:Brand";v="24"', "sec-ch-ua-platform": '"Windows"'},
    {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0",
     "sec-ch-ua": '"Microsoft Edge";v="140", "Chromium";v="140", "Not(A:Brand";v="24"', "sec-ch-ua-platform": '"Windows"'},
    {"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
     "sec-ch-ua": '"Google Chrome";v="141", "Chromium";v="141", "Not(A:Brand";v="24"', "sec-ch-ua-platform": '"Linux"'},
    {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0"},
    {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15"},
]
BASE_HEADERS = {"accept": "*/*", "accept-language": "en-US,en;q=0.9", "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors", "sec-ch-ua-mobile": "?0"}


class BlockedError(RuntimeError):
    """The source kept refusing us (403/429) after all retries."""


def browser_headers() -> dict[str, str]:
    h = dict(BASE_HEADERS)
    h.update(random.choice(BROWSERS))
    if "sec-ch-ua" not in h:
        h.pop("sec-ch-ua-mobile", None)
    return h


def parse_json(text: str) -> Any:
    # Redfin's stingray API prefixes JSON with "{}&&" to defeat JSON hijacking
    if text.startswith("{}&&"):
        text = text[4:]
    return json.loads(text)


class AdaptiveLimiter:
    """Bounded in-flight cap that steps down on sustained blocks or errors and back up after a quiet period.

    levels: caps from highest to lowest (e.g. [200, 100, 50]). A "sustained block" is >= `trip` block
    responses (403/429/captcha) within `window` seconds; >= `error_trip` errors (timeouts, dropped connections,
    5xx, upstream "request aborted/timed out" answers: the source is overloaded) do the same. Either drops one
    level. After `recover` seconds with neither it climbs one level (never above levels[0]). Thread-safe;
    acquire()/release() wrap one HTTP call.
    """

    def __init__(self, name: str, levels: list[int], trip: int = 8, window: float = 60.0, recover: float = 900.0,
                 clock=time.monotonic, error_trip: int | None = None):
        self.name, self.levels, self.trip, self.window, self.recover = name, levels, trip, window, recover
        self.error_trip = error_trip or 2 * trip
        self.level, self.inflight, self.clock = 0, 0, clock
        self.cond = threading.Condition()
        self.blocks: list[float] = []
        self.errors: list[float] = []
        self.last_block = self.last_change = clock()

    @property
    def cap(self) -> int:
        return self.levels[self.level]

    def acquire(self) -> None:
        with self.cond:
            self._maybe_recover()
            while self.inflight >= self.cap:
                self.cond.wait(5)
                self._maybe_recover()
            self.inflight += 1

    def release(self) -> None:
        with self.cond:
            self.inflight -= 1
            self.cond.notify()

    def note_block(self) -> None:
        self._note("blocks", self.trip, "blocks")

    def note_error(self) -> None:
        self._note("errors", self.error_trip, "errors/timeouts")

    def _note(self, attr: str, trip: int, what: str) -> None:
        with self.cond:
            now = self.clock()
            self.last_block = now
            hits = [t for t in getattr(self, attr) if now - t < self.window] + [now]
            setattr(self, attr, hits)
            if len(hits) >= trip and self.level < len(self.levels) - 1 and now - self.last_change > self.window:
                self.level += 1
                self.last_change, self.blocks, self.errors = now, [], []
                print(f"[{self.name}] sustained {what}: in-flight cap -> {self.cap}", flush=True)

    def _maybe_recover(self) -> None:
        now = self.clock()
        if self.level and now - max(self.last_block, self.last_change) >= self.recover:
            self.level -= 1
            self.last_change = now
            print(f"[{self.name}] {self.recover / 60:.0f} min without blocks: in-flight cap -> {self.cap}", flush=True)
            self.cond.notify_all()


def wire_bytes(r: requests.Response) -> int:
    """Approximate bytes on the wire (up + down) for the bandwidth budget: compressed body + headers + request."""
    try:
        body = int(r.headers.get("content-length") or 0) or len(r.content)
    except (TypeError, ValueError):
        body = len(r.content)
    req = r.request
    up = len(req.url or "") + sum(len(k) + len(v) + 4 for k, v in req.headers.items()) + len(req.body or b"") \
        if req is not None else 500
    down = body + sum(len(k) + len(v) + 4 for k, v in r.headers.items()) + 20
    return up + down


def looks_blocked(r: requests.Response) -> bool:
    """Soft block: 200 with an HTML/captcha page where JSON was expected."""
    if r.status_code != 200:
        return False
    head = r.text[:400].lstrip().lower()
    return head.startswith("<") and ("captcha" in head or "access denied" in head or "<html" in head or "<!doctype" in head)


class SourceClient:
    """One per source. Thread-safe; spaces requests at least `min_interval` seconds apart (+ jitter).

    Env vars: <SOURCE>_PROXY (e.g. REDFIN_PROXY) or HTTPS_PROXY/HTTP_PROXY; <SOURCE>_MIN_INTERVAL overrides the pace.
    """

    def __init__(self, name: str, min_interval: float = 1.0, retries: int = 5, timeout: float | None = None,
                 backoff_base: float = 2.0, block_backoff: float = 60.0, sleep=time.sleep):
        self.name = name
        self.min_interval = float(os.environ.get(f"{name.upper()}_MIN_INTERVAL", min_interval))
        # timeout: read seconds, or a (connect, read) pair
        if isinstance(timeout, (tuple, list)):
            connect, read = float(timeout[0]), float(timeout[1])
        else:
            read = float(timeout or os.environ.get(f"{name.upper()}_TIMEOUT", 30))
            connect = min(10.0, read)
        # (connect, read): a dead connection fails in 10 s instead of hanging a county worker
        self.retries, self.timeout = retries, (connect, read)
        self.backoff_base, self.block_backoff = backoff_base, block_backoff
        self.sleep = sleep
        self._lock = threading.Lock()
        self._next_at = 0.0
        self._local = threading.local()
        proxy = os.environ.get(f"{name.upper()}_PROXY") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        self.proxies = {"http": proxy, "https": proxy} if proxy else None
        # ProxyRotator gateway (per-source opt-in; an explicit <SOURCE>_PROXY wins). Shared thread cap + budget.
        explicit = os.environ.get(f"{name.upper()}_PROXY")
        pool = None if explicit else pool_from_env()      # also loads proxy settings from .env
        self.mode = mode_for(name) if pool else "off"
        self.pool = pool if self.mode != "off" else None
        if not self.pool:
            self.mode = "off"
        self._direct_until = 0.0       # hybrid: direct IP backed off (403/429) until this monotonic time
        if self.pool:
            print(f"[{name}] ProxyRotator mode={self.mode} ({self.pool.summary()})", flush=True)
        self.stats = {"requests": 0, "retries": 0, "blocked": 0, "errors": 0, "latency_s": 0.0,
                      "proxied": 0, "shifted_to_proxy": 0}
        self._stats_lock = threading.Lock()
        self.page_parallel = max(1, int(os.environ.get(f"{name.upper()}_PAGE_PARALLEL", 1)))
        cap = int(os.environ.get(f"{name.upper()}_MAX_INFLIGHT", 0) or 0)
        # 20 -> 10 -> 5 per worker style ladder: cap, cap/2, cap/4
        self.limiter = (AdaptiveLimiter(name, [cap, max(1, cap // 2), max(1, cap // 4)], recover=300.0)
                        if cap > 0 else None)

    def _count(self, key: str, v: float = 1) -> None:
        with self._stats_lock:
            self.stats[key] = round(self.stats[key] + v, 3)

    def _session(self) -> requests.Session:
        s = getattr(self._local, "session", None)
        if s is None:
            s = self._local.session = requests.Session()
        return s

    def _wait_turn(self) -> None:
        with self._lock:
            now = time.monotonic()
            at = max(now, self._next_at)
            self._next_at = at + self.min_interval * random.uniform(0.8, 1.4)
        if at > now:
            self.sleep(at - now)

    def request(self, method: str, url: str, headers: dict | None = None, **kw) -> requests.Response:
        last = None
        for attempt in range(self.retries):
            self._wait_turn()
            h = browser_headers()
            h.update(headers or {})
            self._count("requests")
            t0 = time.monotonic()
            pidx, proxies, token = -1, self.proxies, None
            if self.pool:
                token, pidx, purl = self._route()
                if pidx >= 0:
                    proxies = {"http": purl, "https": purl}
                    self._fresh_session_if_rotated()
                    h.setdefault("accept-encoding", "gzip, deflate, br")
            direct = pidx < 0
            if direct and self.limiter:
                self.limiter.acquire()
            try:
                r = self._session().request(method, url, headers=h, proxies=proxies, timeout=self.timeout, **kw)
            except requests.RequestException as e:
                last, blocked = mask(f"{type(e).__name__}: {e}"), False
                self._count("errors")
                if pidx >= 0:
                    self.pool.record(len(url) + 500)
                    self.pool.fail(pidx)
                if direct and self.limiter:
                    self.limiter.note_error()
            else:
                self._count("latency_s", time.monotonic() - t0)
                soft = looks_blocked(r)
                if pidx >= 0:
                    self.pool.record(wire_bytes(r))
                if r.status_code not in RETRY_STATUS and not soft:
                    if pidx >= 0:
                        self.pool.ok(pidx)
                    return r
                blocked = soft or r.status_code in (403, 429)
                if pidx >= 0 and (blocked or r.status_code >= 500):
                    self.pool.fail(pidx)
                last = "HTTP 403 (captcha/HTML)" if soft else f"HTTP {r.status_code}"
                self._count("blocked" if blocked else "errors")
                if direct and blocked and self.mode == "hybrid":
                    self._direct_until = time.monotonic() + self.block_backoff * (attempt + 1)
                    self._count("shifted_to_proxy")
                if direct and self.limiter:
                    (self.limiter.note_block if blocked else self.limiter.note_error)()
            finally:
                if self.pool:
                    self.pool.release_slot(token)
                if direct and self.limiter:
                    self.limiter.release()
            if attempt == self.retries - 1:
                break
            self._count("retries")
            # through the gateway the next attempt is a new IP: short backoff instead of the long block backoff
            # hybrid: a direct block is retried right away through the proxy (new IP), direct itself backs off
            if pidx >= 0:
                wait = self.backoff(attempt, False)
            elif blocked and self.mode == "hybrid" and not self.pool.over_budget():
                wait = 0.0
            else:
                wait = self.backoff(attempt, blocked)
            print(f"[{self.name}] {last} on {method} {url[:120]}; retry {attempt + 1} in {wait:.0f}s", flush=True)
            self.sleep(wait)
        if last and (last.startswith("HTTP 403") or last.startswith("HTTP 429")):
            raise BlockedError(f"[{self.name}] blocked: {last} {url[:200]}")
        raise RuntimeError(f"[{self.name}] failed after {self.retries} attempts: {last} {url[:200]}")

    def _route(self):
        """(slot token, endpoint index, proxy URL); index -1 = go direct. Hybrid: proxy when a global slot is free
        (on top of the direct pool), and *only* proxy while direct is backed off. Over budget: hybrid -> direct."""
        pool = self.pool
        if pool.over_budget():
            if self.mode == "proxy" and not pool.direct_fallback:
                raise ProxyBudgetExceeded(f"[{self.name}] proxy bandwidth budget reached")
            return None, -1, None
        must = self.mode == "proxy" or time.monotonic() < self._direct_until
        token = pool.slot() if must else pool.try_slot()
        if token is None:
            return None, -1, None
        i, purl = pool.current()
        if i < 0:
            pool.release_slot(token)
            return None, -1, None
        self._count("proxied")
        return token, i, purl

    def _fresh_session_if_rotated(self) -> None:
        """After a rotation, drop this thread's keep-alive connection so the gateway assigns a new exit IP."""
        gen = self.pool.generation
        if getattr(self._local, "gen", gen) != gen and getattr(self._local, "session", None) is not None:
            self._local.session.close()
            self._local.session = None
        self._local.gen = gen

    def backoff(self, attempt: int, blocked: bool = False) -> float:
        """Exponential backoff with jitter (so parallel workers that failed together do not retry together)."""
        base = self.block_backoff * (attempt + 1) if blocked else min(60.0, self.backoff_base ** (attempt + 1))
        return base * random.uniform(0.5, 1.5)

    def note_error(self) -> None:
        """An application-level failure the source reported inside a 200 (e.g. upstream timeout): count it and let
        the adaptive cap back off, like a transport error."""
        self._count("errors")
        if self.limiter:
            self.limiter.note_error()

    def request_json(self, method: str, url: str, **kw) -> Any:
        r = self.request(method, url, **kw)
        if r.status_code == 404:
            return None
        return parse_json(r.text)
