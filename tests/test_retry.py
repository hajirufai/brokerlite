"""Tests for retry policies."""

import pytest
from brokerlite.retry import RetryPolicy, RetryStrategy, RetryResult


class TestRetryPolicy:
    def test_should_retry_within_max(self):
        policy = RetryPolicy(max_attempts=3)
        result = policy.should_retry(1)
        assert result.should_retry is True
        assert result.delay_seconds > 0

    def test_should_not_retry_at_max(self):
        policy = RetryPolicy(max_attempts=3)
        result = policy.should_retry(3)
        assert result.should_retry is False

    def test_fixed_delay(self):
        policy = RetryPolicy(
            max_attempts=5,
            initial_delay=1.0,
            strategy=RetryStrategy.FIXED,
        )
        r1 = policy.should_retry(1)
        r2 = policy.should_retry(2)
        assert r1.delay_seconds == r2.delay_seconds == 1.0

    def test_exponential_delay(self):
        policy = RetryPolicy(
            max_attempts=5,
            initial_delay=1.0,
            backoff_multiplier=2.0,
            strategy=RetryStrategy.EXPONENTIAL,
        )
        r1 = policy.should_retry(1)
        r2 = policy.should_retry(2)
        r3 = policy.should_retry(3)
        assert r1.delay_seconds == 1.0
        assert r2.delay_seconds == 2.0
        assert r3.delay_seconds == 4.0

    def test_exponential_max_delay(self):
        policy = RetryPolicy(
            max_attempts=10,
            initial_delay=1.0,
            max_delay=5.0,
            backoff_multiplier=2.0,
            strategy=RetryStrategy.EXPONENTIAL,
        )
        r8 = policy.should_retry(8)
        assert r8.delay_seconds <= 5.0

    def test_jitter_varies(self):
        policy = RetryPolicy(
            max_attempts=5,
            initial_delay=1.0,
            strategy=RetryStrategy.EXPONENTIAL_JITTER,
            jitter_range=0.5,
        )
        delays = [policy.should_retry(2).delay_seconds for _ in range(20)]
        unique = set(f"{d:.6f}" for d in delays)
        assert len(unique) > 1  # should vary

    def test_no_retry_factory(self):
        policy = RetryPolicy.no_retry()
        result = policy.should_retry(1)
        assert result.should_retry is False

    def test_aggressive_factory(self):
        policy = RetryPolicy.aggressive()
        assert policy.max_attempts == 10
        assert policy.initial_delay == 0.1

    def test_conservative_factory(self):
        policy = RetryPolicy.conservative()
        assert policy.max_attempts == 3
        assert policy.initial_delay == 5.0

    def test_delays_for_all_attempts(self):
        policy = RetryPolicy(
            max_attempts=4,
            initial_delay=1.0,
            strategy=RetryStrategy.EXPONENTIAL,
            backoff_multiplier=2.0,
        )
        delays = policy.delays_for_all_attempts()
        assert len(delays) == 3  # attempts 1, 2, 3 get delays

    def test_total_max_delay(self):
        policy = RetryPolicy(
            max_attempts=4,
            initial_delay=1.0,
            strategy=RetryStrategy.EXPONENTIAL,
            backoff_multiplier=2.0,
        )
        total = policy.total_max_delay()
        assert total == 7.0  # 1 + 2 + 4

    def test_to_dict(self):
        policy = RetryPolicy()
        d = policy.to_dict()
        assert "max_attempts" in d
        assert "strategy" in d

    def test_repr(self):
        policy = RetryPolicy()
        assert "RetryPolicy" in repr(policy)


class TestRetryResult:
    def test_attributes(self):
        result = RetryResult(
            should_retry=True,
            delay_seconds=1.5,
            attempt=2,
            max_attempts=5,
            reason="Retry 2/5",
        )
        assert result.should_retry
        assert result.delay_seconds == 1.5
        assert result.attempt == 2
