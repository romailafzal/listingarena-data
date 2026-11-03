"""One pace for all workers of a job: at most `rate` requests a second, and a refusal pauses every worker."""
from __future__ import annotations

import threading
import time


class Pace:
    def __init__(self, rate: float, pause: float = 120, name: str = "", log=print):
        self.interval = 1 / rate if rate > 0 else 0.0
        self.pause, self.name, self.log = pause, name, log
        self._lock = threading.Lock()
        self._next = 0.0
        self._paused_until = 0.0
        self.refusals = 0

    def wait(self) -> None:
        with self._lock:
            start = max(time.time(), self._next, self._paused_until)
            self._next = start + self.interval
        time.sleep(max(0.0, start - time.time()))

    def refused(self, status: int) -> None:
        # once per refusal wave, not once per worker
        with self._lock:
            self.refusals += 1
            if self._paused_until < time.time():
                self._paused_until = time.time() + self.pause
                self.log(f"[{self.name}] HTTP {status}: every request paused {self.pause}s")
