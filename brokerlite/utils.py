"""Utility functions for BrokerLite.

ID generation, hashing, formatting, and other helpers used
across the codebase.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from datetime import datetime, timezone


def generate_id() -> str:
    """Generate a unique ID (UUID4 hex)."""
    return uuid.uuid4().hex


def generate_short_id(length: int = 8) -> str:
    """Generate a short unique ID."""
    return uuid.uuid4().hex[:length]


def consistent_hash(key: str, num_buckets: int) -> int:
    """Consistent hash for partition assignment.

    Given a key, returns a bucket index in [0, num_buckets).
    Same key always maps to the same bucket.
    """
    h = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
    return h % num_buckets


def murmur_hash_2(key: str) -> int:
    """Simple Murmur-like hash for compatibility."""
    data = key.encode("utf-8")
    h = 0x811C9DC5
    for byte in data:
        h ^= byte
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def format_bytes(n: int) -> str:
    """Format byte count as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def format_rate(count: float) -> str:
    """Format messages/second as human-readable string."""
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M/s"
    elif count >= 1_000:
        return f"{count / 1_000:.1f}K/s"
    else:
        return f"{count:.1f}/s"


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable form."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.0f}µs"
    elif seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_timestamp(ts: float) -> str:
    """Format Unix timestamp as ISO 8601 string."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def format_offset(offset: int) -> str:
    """Format an offset for display."""
    return f"#{offset:,}"


def clamp(value: float, minimum: float, maximum: float) -> float:
    """Clamp a value between minimum and maximum."""
    return max(minimum, min(maximum, value))


def retry_with_backoff(fn, max_attempts: int = 3, base_delay: float = 0.1):
    """Retry a function with exponential backoff."""
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception:
            if attempt == max_attempts - 1:
                raise
            time.sleep(base_delay * (2 ** attempt))


class Stopwatch:
    """Simple stopwatch for measuring durations."""

    def __init__(self):
        self._start = 0.0
        self._elapsed = 0.0
        self._running = False

    def start(self) -> Stopwatch:
        self._start = time.monotonic()
        self._running = True
        return self

    def stop(self) -> float:
        if self._running:
            self._elapsed = time.monotonic() - self._start
            self._running = False
        return self._elapsed

    @property
    def elapsed(self) -> float:
        if self._running:
            return time.monotonic() - self._start
        return self._elapsed

    @property
    def elapsed_ms(self) -> float:
        return self.elapsed * 1000

    def __enter__(self) -> Stopwatch:
        return self.start()

    def __exit__(self, *args) -> None:
        self.stop()

    def __repr__(self) -> str:
        return f"Stopwatch(elapsed={format_duration(self.elapsed)})"
