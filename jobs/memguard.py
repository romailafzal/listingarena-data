"""Memory guard: RSS logging and back-pressure when the machine runs low on RAM.

LA_MIN_AVAILABLE_MB (default 3000): new county tasks / load units wait while MemAvailable is below it.
"""
from __future__ import annotations

import os
import time


def rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return 0.0


def available_mb() -> float:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return float("inf")


def wait_for_memory(log=print, what: str = "task", sleep=time.sleep, max_wait: float = 1800) -> None:
    """Block (up to max_wait s) while available RAM is below LA_MIN_AVAILABLE_MB, logging RSS once a minute."""
    floor = float(os.environ.get("LA_MIN_AVAILABLE_MB", 3000))
    waited, last = 0.0, -60.0
    while available_mb() < floor and waited < max_wait:
        if waited - last >= 60:
            log(f"memguard: available {available_mb():.0f} MB < {floor:.0f} MB; pausing new {what} "
                f"(pid {os.getpid()} rss {rss_mb():.0f} MB)")
            last = waited
        sleep(5)
        waited += 5
