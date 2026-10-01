"""smart-cache package."""

from smart_cache.cache import SmartCache, cached
from smart_cache.locks import (
    BaseLock,
    BaseLockProvider,
    DistributedLock,
    DistributedLockProvider,
    InProcessLock,
    InProcessLockProvider,
)

__version__ = "0.1.0"

__all__ = [
    "SmartCache",
    "cached",
    "BaseLock",
    "BaseLockProvider",
    "InProcessLock",
    "DistributedLock",
    "InProcessLockProvider",
    "DistributedLockProvider",
]

