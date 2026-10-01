"""Tests for smart-cache."""

import asyncio
import unittest
from smart_cache import SmartCache, cached


class TestSmartCache(unittest.TestCase):
    def test_cache_hits_and_misses(self):
        async def run_test():
            cache = SmartCache(max_size=10, ttl=10.0)
            compute_count = 0

            @cached(cache)
            async def get_user_data(user_id: int):
                nonlocal compute_count
                compute_count += 1
                return {"id": user_id, "name": f"User_{user_id}"}

            res1 = await get_user_data(1)
            res2 = await get_user_data(1)

            self.assertEqual(res1, {"id": 1, "name": "User_1"})
            self.assertEqual(res2, {"id": 1, "name": "User_1"})
            self.assertEqual(compute_count, 1)

        asyncio.run(run_test())

    def test_stale_while_revalidate(self):
        async def run_test():
            cache = SmartCache(max_size=10, ttl=0.05, stale_ttl=0.5)
            version = 1

            async def fetch_version():
                nonlocal version
                v = version
                version += 1
                return v

            # 1. Fresh fetch (returns 1)
            val1 = await cache.get_or_compute("k1", fetch_version)
            self.assertEqual(val1, 1)

            # 2. Wait for item to become stale but within stale_ttl
            await asyncio.sleep(0.06)

            # 3. Read returns stale value (1) immediately and triggers background revalidation
            val2 = await cache.get_or_compute("k1", fetch_version)
            self.assertEqual(val2, 1)

            # 4. Wait for background task to finish revalidating
            await asyncio.sleep(0.02)

            # 5. Subsequent read returns newly computed fresh value (2)
            val3 = await cache.get_or_compute("k1", fetch_version)
            self.assertEqual(val3, 2)

        asyncio.run(run_test())

    def test_stampede_protection_concurrent_misses(self):
        async def run_test():
            cache = SmartCache(max_size=10, ttl=5.0)
            compute_count = 0

            async def expensive_computation():
                nonlocal compute_count
                compute_count += 1
                await asyncio.sleep(0.05)  # Simulate DB/API latency
                return "heavy_result"

            # 50 concurrent coroutines requesting the same cold key simultaneously
            tasks = [cache.get_or_compute("heavy_key", expensive_computation) for _ in range(50)]
            results = await asyncio.gather(*tasks)

            self.assertEqual(len(results), 50)
            self.assertTrue(all(r == "heavy_result" for r in results))
            # Critical: computation function should only have run ONCE
            self.assertEqual(compute_count, 1)

        asyncio.run(run_test())

    def test_in_process_lock_cleanup(self):
        async def run_test():
            from smart_cache import InProcessLockProvider

            provider = InProcessLockProvider()
            cache = SmartCache(lock_provider=provider)

            for i in range(20):
                await cache.get_or_compute(f"key_{i}", lambda: f"val_{i}")

            # After execution and lock release, active unreferenced locks should be cleaned up
            self.assertEqual(provider.active_lock_count(), 0)

        asyncio.run(run_test())

    def test_distributed_lock_provider(self):
        async def run_test():
            from smart_cache import DistributedLockProvider

            cluster_locks = set()

            async def mock_redis_acquire(key: str, ttl: float) -> bool:
                if key in cluster_locks:
                    return False
                cluster_locks.add(key)
                return True

            async def mock_redis_release(key: str) -> None:
                cluster_locks.discard(key)

            dist_provider = DistributedLockProvider(
                acquire_fn=mock_redis_acquire,
                release_fn=mock_redis_release,
            )

            cache = SmartCache(lock_provider=dist_provider)
            compute_count = 0

            async def compute():
                nonlocal compute_count
                compute_count += 1
                await asyncio.sleep(0.02)
                return "dist_result"

            tasks = [cache.get_or_compute("cluster_key", compute) for _ in range(10)]
            results = await asyncio.gather(*tasks)

            self.assertEqual(len(results), 10)
            self.assertTrue(all(r == "dist_result" for r in results))
            self.assertEqual(compute_count, 1)
            self.assertEqual(len(cluster_locks), 0)

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()

