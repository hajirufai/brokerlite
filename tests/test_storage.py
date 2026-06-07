"""Tests for the write-ahead log and log segments."""

import os
import tempfile
import pytest
from brokerlite.storage import WriteAheadLog, LogSegment


class TestLogSegment:
    def test_append_and_read(self):
        seg = LogSegment(base_offset=0, max_bytes=1024 * 1024)
        offset = seg.append("key-1", b"hello")
        assert offset == 0
        entries = seg.read(0)
        assert len(entries) == 1
        assert entries[0].value == b"hello"
        assert entries[0].key == "key-1"

    def test_sequential_appends(self):
        seg = LogSegment(base_offset=0, max_bytes=1024 * 1024)
        for i in range(5):
            seg.append(f"key-{i}", f"msg-{i}".encode())
        entries = seg.read(0)
        assert len(entries) == 5

    def test_read_from_offset(self):
        seg = LogSegment(base_offset=0, max_bytes=1024 * 1024)
        for i in range(10):
            seg.append(None, f"msg-{i}".encode())
        entries = seg.read(5)
        assert len(entries) == 5

    def test_is_full(self):
        seg = LogSegment(base_offset=0, max_bytes=100)
        for i in range(100):
            if seg.is_full:
                break
            seg.append(None, b"x" * 20)
        assert seg.is_full

    def test_size_bytes(self):
        seg = LogSegment(base_offset=0, max_bytes=1024 * 1024)
        seg.append(None, b"data")
        assert seg.size_bytes > 0

    def test_count(self):
        seg = LogSegment(base_offset=0, max_bytes=1024 * 1024)
        for i in range(3):
            seg.append(None, b"v")
        assert seg.count == 3

    def test_truncate(self):
        seg = LogSegment(base_offset=0, max_bytes=1024 * 1024)
        for i in range(5):
            seg.append(None, b"v")
        removed = seg.truncate(3)
        assert removed == 3
        assert seg.count == 2

    def test_read_at(self):
        seg = LogSegment(base_offset=0, max_bytes=1024 * 1024)
        seg.append("k1", b"first")
        seg.append("k2", b"second")
        entry = seg.read_at(1)
        assert entry is not None
        assert entry.value == b"second"


class TestWriteAheadLog:
    def test_append_and_read(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir)
            offset = wal.append("orders", b"hello")
            assert offset == 0
            entries = wal.read(0)
            assert len(entries) == 1
            assert entries[0].value == b"hello"

    def test_save_and_load_offset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir)
            wal.save_offset("group-1", "orders", 0, 42)
            offset = wal.load_offset("group-1", "orders", 0)
            assert offset == 42

    def test_load_offset_default_is_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir)
            assert wal.load_offset("g", "t", 0) is None

    def test_save_and_load_topic_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir)
            wal.save_topic_config("orders", {
                "partitions": 4,
                "retention_ms": 3600000,
            })
            configs = wal.load_topic_configs()
            assert "orders" in configs
            assert configs["orders"]["partitions"] == 4

    def test_load_empty_topic_configs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir)
            configs = wal.load_topic_configs()
            assert configs == {}

    def test_segment_rotation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir, segment_max_bytes=200)
            for i in range(50):
                wal.append(None, b"x" * 20)
            entries = wal.read(0, max_entries=200)
            assert len(entries) == 50
            assert wal.segment_count > 1

    def test_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir)
            wal.append(None, b"v")
            snap = wal.snapshot()
            assert "segment_count" in snap
            assert "total_entries" in snap
            assert snap["total_entries"] == 1

    def test_compact(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir)
            wal.append("user-1", b"v1")
            wal.append("user-1", b"v2")
            wal.append("user-2", b"v3")
            removed = wal.compact()
            assert removed == 1  # first user-1 entry removed
            assert wal.total_entries == 2

    def test_latest_and_earliest_offset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir)
            assert wal.earliest_offset() == 0
            assert wal.latest_offset() == 0
            wal.append(None, b"v1")
            wal.append(None, b"v2")
            assert wal.latest_offset() == 2

    def test_close(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = WriteAheadLog(data_dir=tmpdir)
            wal.append(None, b"v")
            wal.close()
            assert wal._db is None
