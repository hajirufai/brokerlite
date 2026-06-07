"""Retry policies — configurable retry strategies for failed messages.

Provides exponential backoff, fixed delay, and jittered retry strategies
to handle transient failures without thundering herd effects.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class RetryStrategy(Enum):
    """Retry delay strategy."""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_JITTER = "exponential_jitter"


@dataclass
class RetryResult:
    """Result of a retry decision.

    Attributes:
        should_retry: Whether to attempt redelivery.
        delay_seconds: How long to wait before retrying.
        attempt: Current attempt number.
        max_attempts: Maximum attempts allowed.
        reason: Explanation of the decision.
    """
    should_retry: bool
    delay_seconds: float
    attempt: int
    max_attempts: int
    reason: str


class RetryPolicy:
    """Configurable retry policy for message processing failures.

    Determines whether to retry a failed message and how long to wait
    between attempts.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_multiplier: float = 2.0,
        strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_JITTER,
        jitter_range: float = 0.5,
    ):
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_multiplier = backoff_multiplier
        self.strategy = strategy
        self.jitter_range = jitter_range

    def should_retry(self, attempt: int, error: Optional[str] = None) -> RetryResult:
        """Determine whether to retry and calculate the delay.

        Args:
            attempt: Current attempt number (1-based).
            error: Optional error message for context.
        """
        if attempt >= self.max_attempts:
            return RetryResult(
                should_retry=False,
                delay_seconds=0,
                attempt=attempt,
                max_attempts=self.max_attempts,
                reason=f"Max attempts ({self.max_attempts}) exceeded",
            )

        delay = self._calculate_delay(attempt)

        return RetryResult(
            should_retry=True,
            delay_seconds=delay,
            attempt=attempt,
            max_attempts=self.max_attempts,
            reason=f"Retry {attempt}/{self.max_attempts} after {delay:.2f}s",
        )

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate the delay for the given attempt."""
        if self.strategy == RetryStrategy.FIXED:
            return min(self.initial_delay, self.max_delay)

        elif self.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.initial_delay * (self.backoff_multiplier ** (attempt - 1))
            return min(delay, self.max_delay)

        elif self.strategy == RetryStrategy.EXPONENTIAL_JITTER:
            base_delay = self.initial_delay * (self.backoff_multiplier ** (attempt - 1))
            base_delay = min(base_delay, self.max_delay)
            jitter = random.uniform(-self.jitter_range, self.jitter_range)
            return max(0, base_delay * (1 + jitter))

        return self.initial_delay

    def delays_for_all_attempts(self) -> list[float]:
        """Calculate delays for all attempts (useful for documentation)."""
        delays = []
        for attempt in range(1, self.max_attempts + 1):
            result = self.should_retry(attempt)
            if result.should_retry:
                delays.append(result.delay_seconds)
        return delays

    def total_max_delay(self) -> float:
        """Maximum total time across all retries (worst case)."""
        total = 0.0
        for attempt in range(1, self.max_attempts):
            if self.strategy == RetryStrategy.FIXED:
                total += min(self.initial_delay, self.max_delay)
            elif self.strategy in (RetryStrategy.EXPONENTIAL, RetryStrategy.EXPONENTIAL_JITTER):
                delay = self.initial_delay * (self.backoff_multiplier ** (attempt - 1))
                total += min(delay, self.max_delay)
        return total

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_attempts": self.max_attempts,
            "initial_delay": self.initial_delay,
            "max_delay": self.max_delay,
            "backoff_multiplier": self.backoff_multiplier,
            "strategy": self.strategy.value,
            "jitter_range": self.jitter_range,
            "total_max_delay": self.total_max_delay(),
        }

    @classmethod
    def no_retry(cls) -> RetryPolicy:
        """Create a policy that never retries."""
        return cls(max_attempts=1)

    @classmethod
    def aggressive(cls) -> RetryPolicy:
        """Create a policy with many fast retries."""
        return cls(
            max_attempts=10,
            initial_delay=0.1,
            max_delay=5.0,
            backoff_multiplier=1.5,
            strategy=RetryStrategy.EXPONENTIAL_JITTER,
        )

    @classmethod
    def conservative(cls) -> RetryPolicy:
        """Create a policy with few slow retries."""
        return cls(
            max_attempts=3,
            initial_delay=5.0,
            max_delay=120.0,
            backoff_multiplier=3.0,
            strategy=RetryStrategy.EXPONENTIAL,
        )

    def __repr__(self) -> str:
        return (
            f"RetryPolicy(max_attempts={self.max_attempts}, "
            f"strategy={self.strategy.value}, "
            f"initial_delay={self.initial_delay}s)"
        )
