"""Async SmartCache with Stale-While-Revalidate and Stampede Protection."""

from __future__ import annotations

import asyncio
import functools
import inspect
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


@dataclass
class CacheEntry:
    value: Any
    created_at: float
    ttl: float
    stale_ttl: float

    @property
    def is_fresh(self) -> bool:
        return (time.time() - self.created_at) <= self.ttl

    @property
    def is_stale_allowed(self) -> bool:
        return (time.time() - self.created_at) <= (self.ttl + self.stale_ttl)


class SmartCache:
    """In-memory LRU Cache with TTL, SWR background updates, and Mutex Locks."""

    def __init__(self, max_size: int = 1000, ttl: float = 60.0, stale_ttl: float = 60.0):
        self.max_size = max_size
        self.default_ttl = ttl
        self.default_stale_ttl = stale_ttl
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
        self._locks: Dict[str, asyncio.Lock] = {}

    def _get_lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: Optional[float] = None,
        stale_ttl: Optional[float] = None,
    ) -> Any:
        _ttl = ttl if ttl is not None else self.default_ttl
        _stale = stale_ttl if stale_ttl is not None else self.default_stale_ttl

        # 1. Check existing entry
        if key in self._store:
            entry = self._store[key]
            self._store.move_to_end(key)

            if entry.is_fresh:
                return entry.value

            if entry.is_stale_allowed:
                # Revalidate in background asynchronously!
                asyncio.create_task(self._revalidate(key, compute_fn, _ttl, _stale))
                return entry.value

        # 2. Need synchronous computation with stampede lock
        lock = self._get_lock(key)
        async with lock:
            # Double check after acquiring lock
            if key in self._store:
                entry = self._store[key]
                if entry.is_fresh:
                    return entry.value

            # Compute
            if inspect.iscoroutinefunction(compute_fn):
                value = await compute_fn()
            else:
                res = compute_fn()
                value = await res if inspect.isawaitable(res) else res

            self.set(key, value, ttl=_ttl, stale_ttl=_stale)
            return value

    async def _revalidate(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: float,
        stale_ttl: float,
    ) -> None:
        lock = self._get_lock(key)
        if lock.locked():
            return  # Already revalidating

        async with lock:
            try:
                if inspect.iscoroutinefunction(compute_fn):
                    value = await compute_fn()
                else:
                    res = compute_fn()
                    value = await res if inspect.isawaitable(res) else res
                self.set(key, value, ttl=ttl, stale_ttl=stale_ttl)
            except Exception:
                pass  # Keep stale value on background error

    def set(self, key: str, value: Any, ttl: Optional[float] = None, stale_ttl: Optional[float] = None) -> None:
        _ttl = ttl if ttl is not None else self.default_ttl
        _stale = stale_ttl if stale_ttl is not None else self.default_stale_ttl

        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = CacheEntry(
            value=value,
            created_at=time.time(),
            ttl=_ttl,
            stale_ttl=_stale,
        )

        if len(self._store) > self.max_size:
            self._store.popitem(last=False)

    def clear(self) -> None:
        self._store.clear()
        self._locks.clear()


def cached(cache: SmartCache, ttl: Optional[float] = None, stale_ttl: Optional[float] = None) -> Callable[[F], F]:
    """Decorator to cache async function results in a SmartCache instance."""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            key_parts = [func.__qualname__]
            key_parts.extend(str(a) for a in args)
            key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
            cache_key = ":".join(key_parts)

            return await cache.get_or_compute(
                key=cache_key,
                compute_fn=lambda: func(*args, **kwargs),
                ttl=ttl,
                stale_ttl=stale_ttl,
            )
        return wrapper  # type: ignore
    return decorator
