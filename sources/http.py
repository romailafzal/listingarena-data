"""Shared HTTP client: per-source pacing + in-flight cap, retry with backoff, block detection, proxies, rotating headers.

Concurrency knobs (env, per source; NAME = REDFIN / REALTOR / ...):
  NAME_MIN_INTERVAL  global spacing between request *starts* (s). 0 = no pacing. Slots are reserved under a
                     lock but the wait happens outside it, so with 0 nothing is serialized.
  NAME_MAX_INFLIGHT  cap on simultaneous HTTP calls for this client (adaptive, see AdaptiveLimiter). Unset = no cap.
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
    """Bounded in-flight cap that steps down on sustained blocks and back up after a quiet period.

    levels: caps from highest to lowest (e.g. [200, 100, 50]). A "sustained block" is >= `trip` block
    responses (403/429/captcha) within `window` seconds; it drops one level. After `recover` seconds with no
    block it climbs one level (never above levels[0]). Thread-safe; acquire()/release() wrap one HTTP call.
    """

    def __init__(self, name: str, levels: list[int], trip: int = 8, window: float = 60.0, recover: float = 900.0,
                 clock=time.monotonic):
        self.name, self.levels, self.trip, self.window, self.recover = name, levels, trip, window, recover
        self.level, self.inflight, self.clock = 0, 0, clock
        self.cond = threading.Condition()
        self.blocks: list[float] = []
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
        with self.cond:
            now = self.clock()
            self.last_block = now
            self.blocks = [t for t in self.blocks if now - t < self.window] + [now]
            if len(self.blocks) >= self.trip and self.level < len(self.levels) - 1 and now - self.last_change > self.window:
                self.level += 1
                self.last_change, self.blocks = now, []
                print(f"[{self.name}] sustained blocks: in-flight cap -> {self.cap}", flush=True)

    def _maybe_recover(self) -> None:
        now = self.clock()
        if self.level and now - max(self.last_block, self.last_change) >= self.recover:
            self.level -= 1
            self.last_change = now
            print(f"[{self.name}] {self.recover / 60:.0f} min without blocks: in-flight cap -> {self.cap}", flush=True)
            self.cond.notify_all()


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

    def __init__(self, name: str, min_interval: float = 1.0, retries: int = 5, timeout: float = 60,
                 backoff_base: float = 2.0, block_backoff: float = 60.0, sleep=time.sleep):
        self.name = name
        self.min_interval = float(os.environ.get(f"{name.upper()}_MIN_INTERVAL", min_interval))
        self.retries, self.timeout = retries, timeout
        self.backoff_base, self.block_backoff = backoff_base, block_backoff
        self.sleep = sleep
        self._lock = threading.Lock()
        self._next_at = 0.0
        self._local = threading.local()
        proxy = os.environ.get(f"{name.upper()}_PROXY") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        self.proxies = {"http": proxy, "https": proxy} if proxy else None
        self.stats = {"requests": 0, "retries": 0, "blocked": 0, "errors": 0, "latency_s": 0.0}
        self._stats_lock = threading.Lock()
        self.page_parallel = max(1, int(os.environ.get(f"{name.upper()}_PAGE_PARALLEL", 1)))
        cap = int(os.environ.get(f"{name.upper()}_MAX_INFLIGHT", 0) or 0)
        # 20 -> 10 -> 5 per worker style ladder: cap, cap/2, cap/4
        self.limiter = AdaptiveLimiter(name, [cap, max(1, cap // 2), max(1, cap // 4)]) if cap > 0 else None

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
            if self.limiter:
                self.limiter.acquire()
            try:
                r = self._session().request(method, url, headers=h, proxies=self.proxies, timeout=self.timeout, **kw)
            except requests.RequestException as e:
                last, blocked = f"{type(e).__name__}: {e}", False
                self._count("errors")
            else:
                self._count("latency_s", time.monotonic() - t0)
                soft = looks_blocked(r)
                if r.status_code not in RETRY_STATUS and not soft:
                    return r
                blocked = soft or r.status_code in (403, 429)
                last = "HTTP 403 (captcha/HTML)" if soft else f"HTTP {r.status_code}"
                self._count("blocked" if blocked else "errors")
                if blocked and self.limiter:
                    self.limiter.note_block()
            finally:
                if self.limiter:
                    self.limiter.release()
            if attempt == self.retries - 1:
                break
            self._count("retries")
            wait = (self.block_backoff * (attempt + 1) if blocked else self.backoff_base ** (attempt + 1)) * random.uniform(1, 1.3)
            print(f"[{self.name}] {last} on {method} {url[:120]}; retry {attempt + 1} in {wait:.0f}s", flush=True)
            self.sleep(wait)
        if last and (last.startswith("HTTP 403") or last.startswith("HTTP 429")):
            raise BlockedError(f"[{self.name}] blocked: {last} {url[:200]}")
        raise RuntimeError(f"[{self.name}] failed after {self.retries} attempts: {last} {url[:200]}")

    def request_json(self, method: str, url: str, **kw) -> Any:
        r = self.request(method, url, **kw)
        if r.status_code == 404:
            return None
        return parse_json(r.text)
