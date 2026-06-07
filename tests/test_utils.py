"""Tests for utility functions."""

import pytest
from brokerlite.utils import (
    generate_id, generate_short_id, consistent_hash, murmur_hash_2,
    format_bytes, format_rate, format_duration, format_timestamp,
    clamp, Stopwatch,
)


class TestIdGeneration:
    def test_unique_ids(self):
        ids = {generate_id() for _ in range(100)}
        assert len(ids) == 100

    def test_short_id_length(self):
        assert len(generate_short_id(8)) == 8
        assert len(generate_short_id(4)) == 4


class TestConsistentHash:
    def test_same_key_same_bucket(self):
        for _ in range(10):
            assert consistent_hash("user-123", 10) == consistent_hash("user-123", 10)

    def test_distributes_across_buckets(self):
        buckets = {consistent_hash(f"key-{i}", 10) for i in range(100)}
        assert len(buckets) > 5


class TestMurmurHash:
    def test_deterministic(self):
        h1 = murmur_hash_2("hello")
        h2 = murmur_hash_2("hello")
        assert h1 == h2

    def test_different_values(self):
        assert murmur_hash_2("hello") != murmur_hash_2("world")


class TestFormatBytes:
    def test_bytes(self):
        assert "B" in format_bytes(500)

    def test_kilobytes(self):
        assert "KB" in format_bytes(2048)

    def test_megabytes(self):
        assert "MB" in format_bytes(5 * 1024 * 1024)


class TestFormatRate:
    def test_low_rate(self):
        assert "/s" in format_rate(50)

    def test_kilo_rate(self):
        assert "K/s" in format_rate(5000)

    def test_mega_rate(self):
        assert "M/s" in format_rate(2_000_000)


class TestFormatDuration:
    def test_microseconds(self):
        assert "µs" in format_duration(0.0001)

    def test_milliseconds(self):
        assert "ms" in format_duration(0.5)

    def test_seconds(self):
        assert "s" in format_duration(30)

    def test_minutes(self):
        assert "m" in format_duration(90)

    def test_hours(self):
        assert "h" in format_duration(7200)


class TestFormatTimestamp:
    def test_format(self):
        result = format_timestamp(1000000.0)
        assert "1970" in result
        assert "Z" in result


class TestClamp:
    def test_within_range(self):
        assert clamp(5, 0, 10) == 5

    def test_below_min(self):
        assert clamp(-5, 0, 10) == 0

    def test_above_max(self):
        assert clamp(15, 0, 10) == 10


class TestStopwatch:
    def test_basic_timing(self):
        sw = Stopwatch()
        sw.start()
        import time
        time.sleep(0.01)
        elapsed = sw.stop()
        assert elapsed > 0

    def test_context_manager(self):
        with Stopwatch() as sw:
            pass
        assert sw.elapsed >= 0

    def test_elapsed_ms(self):
        sw = Stopwatch()
        sw.start()
        sw.stop()
        assert sw.elapsed_ms >= 0

    def test_repr(self):
        sw = Stopwatch()
        assert "Stopwatch" in repr(sw)
