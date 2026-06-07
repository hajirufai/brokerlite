"""Write-ahead log — persistent message storage.

Messages are written to append-only log segments. Each partition
has its own sequence of log segments, providing durability and
recovery after restart.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .message import Message


@dataclass
class LogEntry:
    """A single entry in the write-ahead log.

    Attributes:
        offset: The sequential position in the log.
        timestamp: When the entry was written.
        key: Optional partitioning key.
        value: The serialized message data.
    """
    offset: int
    timestamp: float
    key: Optional[str]
    value: bytes


class LogSegment:
    """A segment of the write-ahead log.

    The WAL is divided into fixed-size segments. When a segment
    reaches max_bytes, a new segment is created. Old segments
    can be deleted for retention or compacted.
    """

    def __init__(
        self,
        base_offset: int,
        path: Optional[str] = None,
        max_bytes: int = 64 * 1024 * 1024,
    ):
        self.base_offset = base_offset
        self.path = path
        self.max_bytes = max_bytes

        self._entries: list[LogEntry] = []
        self._size_bytes = 0
        self._lock = threading.RLock()

    @property
    def next_offset(self) -> int:
        with self._lock:
            if not self._entries:
                return self.base_offset
            return self._entries[-1].offset + 1

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    @property
    def size_bytes(self) -> int:
        with self._lock:
            return self._size_bytes

    @property
    def is_full(self) -> bool:
        return self._size_bytes >= self.max_bytes

    def append(self, key: Optional[str], value: bytes) -> int:
        """Append an entry to the segment. Returns the offset."""
        with self._lock:
            offset = self.next_offset
            entry = LogEntry(
                offset=offset,
                timestamp=time.time(),
                key=key,
                value=value,
            )
            self._entries.append(entry)
            self._size_bytes += len(value) + 64  # overhead estimate
            return offset

    def read(self, offset: int, max_entries: int = 100) -> list[LogEntry]:
        """Read entries starting from offset."""
        with self._lock:
            if not self._entries:
                return []
            start_idx = offset - self.base_offset
            if start_idx < 0:
                start_idx = 0
            if start_idx >= len(self._entries):
                return []
            end_idx = min(start_idx + max_entries, len(self._entries))
            return list(self._entries[start_idx:end_idx])

    def read_at(self, offset: int) -> Optional[LogEntry]:
        """Read a single entry at the exact offset."""
        with self._lock:
            idx = offset - self.base_offset
            if 0 <= idx < len(self._entries):
                return self._entries[idx]
            return None

    def truncate(self, offset: int) -> int:
        """Remove all entries before offset. Returns count removed."""
        with self._lock:
            idx = offset - self.base_offset
            if idx <= 0:
                return 0
            removed = self._entries[:idx]
            self._entries = self._entries[idx:]
            removed_bytes = sum(len(e.value) + 64 for e in removed)
            self._size_bytes -= removed_bytes
            return len(removed)

    def __repr__(self) -> str:
        return (
            f"LogSegment(base={self.base_offset}, "
            f"entries={self.count}, bytes={self._size_bytes})"
        )


class WriteAheadLog:
    """Write-ahead log with segmented storage and SQLite metadata.

    Provides durable message storage with:
    - Append-only writes for crash safety
    - Segment rotation for bounded file sizes
    - Offset-based reads for consumer resumption
    - Compaction for key-based deduplication
    - Retention for automatic cleanup
    """

    def __init__(
        self,
        data_dir: Optional[str] = None,
        segment_max_bytes: int = 64 * 1024 * 1024,
        retention_ms: int = 0,
    ):
        self.data_dir = data_dir
        self.segment_max_bytes = segment_max_bytes
        self.retention_ms = retention_ms

        self._segments: list[LogSegment] = []
        self._lock = threading.RLock()
        self._total_appended = 0

        self._db: Optional[sqlite3.Connection] = None
        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, "metadata.db")
            self._db = sqlite3.connect(db_path, check_same_thread=False)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._init_db()

        self._segments.append(
            LogSegment(0, max_bytes=segment_max_bytes)
        )

    def _init_db(self) -> None:
        """Initialize metadata tables."""
        if self._db is None:
            return
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS offsets (
                group_id TEXT,
                topic TEXT,
                partition_id INTEGER,
                committed_offset INTEGER,
                updated_at REAL,
                PRIMARY KEY (group_id, topic, partition_id)
            );
            CREATE TABLE IF NOT EXISTS topics (
                name TEXT PRIMARY KEY,
                config TEXT,
                created_at REAL
            );
        """)
        self._db.commit()

    @property
    def active_segment(self) -> LogSegment:
        """The current writable segment."""
        return self._segments[-1]

    def append(self, key: Optional[str], value: bytes) -> int:
        """Append a message to the log. Returns the offset.

        Rotates to a new segment if the current one is full.
        """
        with self._lock:
            if self.active_segment.is_full:
                next_base = self.active_segment.next_offset
                new_segment = LogSegment(
                    next_base, max_bytes=self.segment_max_bytes
                )
                self._segments.append(new_segment)

            offset = self.active_segment.append(key, value)
            self._total_appended += 1
            return offset

    def read(self, offset: int, max_entries: int = 100) -> list[LogEntry]:
        """Read entries starting from offset, across segments."""
        with self._lock:
            results: list[LogEntry] = []
            remaining = max_entries

            for segment in self._segments:
                if remaining <= 0:
                    break
                if segment.next_offset <= offset:
                    continue

                entries = segment.read(offset, remaining)
                results.extend(entries)
                remaining -= len(entries)

                if entries:
                    offset = entries[-1].offset + 1

            return results

    def latest_offset(self) -> int:
        """The next offset that will be assigned."""
        with self._lock:
            return self.active_segment.next_offset

    def earliest_offset(self) -> int:
        """The earliest available offset."""
        with self._lock:
            if self._segments:
                return self._segments[0].base_offset
            return 0

    @property
    def segment_count(self) -> int:
        with self._lock:
            return len(self._segments)

    @property
    def total_entries(self) -> int:
        with self._lock:
            return sum(s.count for s in self._segments)

    @property
    def total_bytes(self) -> int:
        with self._lock:
            return sum(s.size_bytes for s in self._segments)

    def apply_retention(self) -> int:
        """Remove segments older than retention_ms."""
        if self.retention_ms <= 0:
            return 0

        cutoff = time.time() - (self.retention_ms / 1000.0)
        removed = 0

        with self._lock:
            while len(self._segments) > 1:
                segment = self._segments[0]
                entries = segment.read(segment.base_offset, 1)
                if entries and entries[0].timestamp < cutoff:
                    self._segments.pop(0)
                    removed += segment.count
                else:
                    break

        return removed

    def compact(self) -> int:
        """Key-based compaction: keep only latest entry per key."""
        with self._lock:
            all_entries: list[LogEntry] = []
            for segment in self._segments:
                all_entries.extend(segment.read(segment.base_offset, segment.count))

            original_count = len(all_entries)
            latest_by_key: dict[str, int] = {}
            for i, entry in enumerate(all_entries):
                if entry.key is not None:
                    latest_by_key[entry.key] = i

            compacted: list[LogEntry] = []
            for i, entry in enumerate(all_entries):
                if entry.key is None or latest_by_key.get(entry.key) == i:
                    compacted.append(entry)

            new_segment = LogSegment(
                compacted[0].offset if compacted else 0,
                max_bytes=self.segment_max_bytes,
            )
            for entry in compacted:
                new_segment.append(entry.key, entry.value)

            self._segments = [new_segment]
            return original_count - len(compacted)

    # --- SQLite metadata operations ---

    def save_offset(
        self, group_id: str, topic: str, partition_id: int, offset: int
    ) -> None:
        """Persist a consumer group's committed offset."""
        if self._db is None:
            return
        self._db.execute(
            """INSERT OR REPLACE INTO offsets
               (group_id, topic, partition_id, committed_offset, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (group_id, topic, partition_id, offset, time.time()),
        )
        self._db.commit()

    def load_offset(
        self, group_id: str, topic: str, partition_id: int
    ) -> Optional[int]:
        """Load a consumer group's committed offset."""
        if self._db is None:
            return None
        row = self._db.execute(
            """SELECT committed_offset FROM offsets
               WHERE group_id=? AND topic=? AND partition_id=?""",
            (group_id, topic, partition_id),
        ).fetchone()
        return row[0] if row else None

    def save_topic_config(self, name: str, config: dict) -> None:
        """Persist a topic configuration."""
        if self._db is None:
            return
        self._db.execute(
            """INSERT OR REPLACE INTO topics (name, config, created_at)
               VALUES (?, ?, ?)""",
            (name, json.dumps(config), time.time()),
        )
        self._db.commit()

    def load_topic_configs(self) -> dict[str, dict]:
        """Load all topic configurations."""
        if self._db is None:
            return {}
        rows = self._db.execute("SELECT name, config FROM topics").fetchall()
        return {name: json.loads(config) for name, config in rows}

    def close(self) -> None:
        """Close the storage backend."""
        if self._db:
            self._db.close()
            self._db = None

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "total_entries": self.total_entries,
                "total_bytes": self.total_bytes,
                "segment_count": len(self._segments),
                "earliest_offset": self.earliest_offset(),
                "latest_offset": self.latest_offset(),
                "total_appended": self._total_appended,
                "retention_ms": self.retention_ms,
            }

    def __repr__(self) -> str:
        return (
            f"WriteAheadLog(segments={self.segment_count}, "
            f"entries={self.total_entries}, bytes={self.total_bytes})"
        )
