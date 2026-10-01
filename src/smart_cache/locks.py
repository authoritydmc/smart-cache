"""In-process and distributed mutex locks for cache stampede protection."""

from __future__ import annotations

import abc
import asyncio
import time
from typing import Any, Callable, Dict, Optional


class BaseLock(abc.ABC):
    """Abstract base class for stampede mutex locks."""

    @abc.abstractmethod
    async def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquires the lock. Returns True if successfully acquired."""
        raise NotImplementedError

    @abc.abstractmethod
    async def release(self) -> None:
        """Releases the lock."""
        raise NotImplementedError

    @abc.abstractmethod
    def is_locked(self) -> bool:
        """Returns True if the lock is currently held."""
        raise NotImplementedError

    async def __aenter__(self) -> "BaseLock":
        await self.acquire(blocking=True)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.release()


class InProcessLock(BaseLock):
    """In-process async mutex lock with ref-counting for memory reclamation."""

    def __init__(self, key: str, on_release: Optional[Callable[[str], None]] = None):
        self.key = key
        self._lock = asyncio.Lock()
        self._on_release = on_release
        self.ref_count = 0

    def is_locked(self) -> bool:
        return self._lock.locked()

    async def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        if not blocking:
            if self._lock.locked():
                return False
            # Try non-blocking acquire
            try:
                # In asyncio, acquire() is a coroutine
                acquired = await asyncio.wait_for(self._lock.acquire(), timeout=0.0001)
                return True
            except asyncio.TimeoutError:
                return False

        if timeout is not None:
            try:
                await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
                return True
            except asyncio.TimeoutError:
                return False

        await self._lock.acquire()
        return True

    async def release(self) -> None:
        if self._lock.locked():
            self._lock.release()
        if self._on_release:
            self._on_release(self.key)


class DistributedLock(BaseLock):
    """Async distributed mutex lock with lease TTL and backoff retries."""

    def __init__(
        self,
        key: str,
        acquire_fn: Optional[Callable[[str, float], Any]] = None,
        release_fn: Optional[Callable[[str], Any]] = None,
        ttl: float = 30.0,
        retry_interval: float = 0.05,
    ):
        self.key = key
        self.acquire_fn = acquire_fn
        self.release_fn = release_fn
        self.ttl = ttl
        self.retry_interval = retry_interval
        self._is_held = False
        self._lock_expires_at: float = 0.0

    def is_locked(self) -> bool:
        if self._is_held and time.time() < self._lock_expires_at:
            return True
        return False

    async def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        start_time = time.time()
        while True:
            success = False
            if self.acquire_fn:
                res = self.acquire_fn(self.key, self.ttl)
                success = await res if asyncio.iscoroutine(res) else bool(res)
            else:
                success = True

            if success:
                self._is_held = True
                self._lock_expires_at = time.time() + self.ttl
                return True

            if not blocking:
                return False

            if timeout is not None and (time.time() - start_time) >= timeout:
                return False

            await asyncio.sleep(self.retry_interval)

    async def release(self) -> None:
        if self._is_held:
            self._is_held = False
            if self.release_fn:
                res = self.release_fn(self.key)
                if asyncio.iscoroutine(res):
                    await res


class BaseLockProvider(abc.ABC):
    """Factory and registry for key-scoped locks."""

    @abc.abstractmethod
    def get_lock(self, key: str) -> BaseLock:
        """Returns a lock instance for the given cache key."""
        raise NotImplementedError


class InProcessLockProvider(BaseLockProvider):
    """In-process lock registry with automatic cleanup of unreferenced locks."""

    def __init__(self):
        self._locks: Dict[str, InProcessLock] = {}
        self._lock_guard = asyncio.Lock()

    def get_lock(self, key: str) -> InProcessLock:
        if key not in self._locks:
            self._locks[key] = InProcessLock(key, on_release=self._maybe_cleanup)
        lock = self._locks[key]
        lock.ref_count += 1
        return lock

    def _maybe_cleanup(self, key: str) -> None:
        if key in self._locks:
            lock = self._locks[key]
            lock.ref_count = max(0, lock.ref_count - 1)
            if lock.ref_count == 0 and not lock.is_locked():
                self._locks.pop(key, None)

    def active_lock_count(self) -> int:
        return len(self._locks)


class DistributedLockProvider(BaseLockProvider):
    """Distributed lock provider using custom acquire and release hooks."""

    def __init__(
        self,
        acquire_fn: Callable[[str, float], Any],
        release_fn: Callable[[str], Any],
        ttl: float = 30.0,
        retry_interval: float = 0.05,
    ):
        self.acquire_fn = acquire_fn
        self.release_fn = release_fn
        self.ttl = ttl
        self.retry_interval = retry_interval

    def get_lock(self, key: str) -> DistributedLock:
        return DistributedLock(
            key=key,
            acquire_fn=self.acquire_fn,
            release_fn=self.release_fn,
            ttl=self.ttl,
            retry_interval=self.retry_interval,
        )
