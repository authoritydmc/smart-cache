# ⚡ smart-cache

**Async Hybrid Cache with Stale-While-Revalidate & Stampede Protection for Python**

`smart-cache` delivers microsecond cached reads with background asynchronous revalidation (SWR) and in-process mutexes to prevent dog-piling / cache stampedes.

---

## ✨ Features

- 🚀 **Stale-While-Revalidate (SWR):** Instant responses for stale items while fresh data computes asynchronously in the background.
- 🛡️ **Stampede / Thundering Herd Protection:** Guarantees only one worker computes expensive operations concurrently.
- ⏱️ **TTL & LRU Eviction:** Configurable expiration and capacity limits.
- 🐍 **Native Async/Await & Sync:** Clean decorators for async coroutines.

---

## 🚀 Quickstart

```python
import asyncio
from smart_cache import SmartCache, cached

cache = SmartCache(max_size=1000, ttl=60, stale_ttl=120)

@cached(cache)
async def fetch_expensive_stats(metric_id: str):
    await asyncio.sleep(2) # Expensive query
    return {"metric_id": metric_id, "value": 42}
```

---

## 📄 License

MIT License.
