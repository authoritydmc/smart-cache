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


if __name__ == "__main__":
    unittest.main()
