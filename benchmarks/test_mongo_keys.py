"""The FastAPI MongoDB baseline reserves keys in blocks like Siderite."""

import asyncio
import importlib.util
import unittest
from pathlib import Path

# Loaded by path: the app's modules sit in a directory named ``fastapi``.
_SPEC = importlib.util.spec_from_file_location(
    "mongo_keys", Path(__file__).parent / "fastapi" / "mongo_keys.py"
)
mongo_keys = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mongo_keys)


class FakeCounters:
    """In-memory ``siderite_counters`` with an atomic upserting ``$inc``."""

    def __init__(self, seeded=None):
        """Create the collection, optionally with a seeded ``todos`` counter.

        Args:
            seeded: Initial ``seq`` of the ``todos`` counter, or None.
        """
        self.docs = {} if seeded is None else {"todos": {"seq": seeded}}
        self.calls = []

    async def find_one_and_update(
        self, query, update, upsert=False, return_document=False
    ):
        """Apply ``$inc`` after yielding to the loop, like a round trip.

        Args:
            query: ``{_id: name}`` filter.
            update: ``{$inc: {seq: n}}`` update.
            upsert: Must be true.
            return_document: Must be true (``ReturnDocument.AFTER``).

        Returns:
            The counter document after the update.
        """
        self.calls.append((query, update, upsert, return_document))
        await asyncio.sleep(0)
        doc = self.docs.setdefault(query["_id"], {"seq": 0})
        doc["seq"] += update["$inc"]["seq"]
        await asyncio.sleep(0)
        return dict(doc)


def take(allocator, count):
    """Return ``count`` keys taken one after another.

    Args:
        allocator: Allocator under test.
        count: Number of keys.

    Returns:
        The keys in the order they were handed out.
    """
    async def run():
        return [await allocator.next_key() for _ in range(count)]

    return asyncio.run(run())


class KeyBlockAllocatorTests(unittest.TestCase):
    """Keys, counter calls and the counter document match Siderite."""

    def test_keys_within_a_block_follow_the_seeded_count(self):
        counters = FakeCounters(seeded=100)
        allocator = mongo_keys.KeyBlockAllocator(counters, "todos")
        self.assertEqual(take(allocator, 3), [101, 102, 103])
        self.assertEqual(
            counters.calls,
            [({"_id": "todos"}, {"$inc": {"seq": 64}}, True, True)],
        )
        # The counter holds the last reserved key, as in Siderite.
        self.assertEqual(counters.docs["todos"]["seq"], 164)

    def test_a_missing_counter_starts_at_one(self):
        counters = FakeCounters()
        allocator = mongo_keys.KeyBlockAllocator(counters, "todos")
        self.assertEqual(take(allocator, 2), [1, 2])

    def test_refills_after_the_block_runs_out(self):
        counters = FakeCounters(seeded=100)
        allocator = mongo_keys.KeyBlockAllocator(counters, "todos")
        keys = take(allocator, 65)
        self.assertEqual(keys, list(range(101, 166)))
        self.assertEqual(len(counters.calls), 2)
        self.assertEqual(counters.docs["todos"]["seq"], 228)

    def test_processes_get_separate_blocks(self):
        counters = FakeCounters(seeded=100)
        first = mongo_keys.KeyBlockAllocator(counters, "todos")
        second = mongo_keys.KeyBlockAllocator(counters, "todos")
        self.assertEqual(take(first, 1), [101])
        self.assertEqual(take(second, 1), [165])
        self.assertEqual(take(first, 1), [102])

    def test_concurrent_tasks_get_unique_keys_one_call_per_block(self):
        counters = FakeCounters(seeded=100)
        allocator = mongo_keys.KeyBlockAllocator(counters, "todos")

        async def run():
            return await asyncio.gather(
                *(allocator.next_key() for _ in range(200))
            )

        keys = asyncio.run(run())
        self.assertEqual(sorted(keys), list(range(101, 301)))
        # 200 keys need four 64-key blocks, each reserved exactly once.
        self.assertEqual(len(counters.calls), 4)
        self.assertEqual(counters.docs["todos"]["seq"], 356)

    def test_block_size_is_at_least_one(self):
        counters = FakeCounters()
        allocator = mongo_keys.KeyBlockAllocator(counters, "todos", 0)
        self.assertEqual(take(allocator, 2), [1, 2])
        self.assertEqual(len(counters.calls), 2)


if __name__ == "__main__":
    unittest.main()
