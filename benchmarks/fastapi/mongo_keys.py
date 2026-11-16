"""Block allocation of generated integer keys for the MongoDB baseline.

Mirrors Siderite's MongoDB backend
(``crates/siderite-backends/src/mongodb/keys.rs``): one ``$inc`` of
``block_size`` on the ``{_id: <collection>, seq: <last key>}`` document in
``siderite_counters`` reserves the keys ``seq - block_size + 1 ..= seq``,
and inserts take keys from memory until the block runs out. The counter
document keeps its format, so a counter seeded at ``seq = N`` hands out
keys from ``N + 1``.

Each allocator instance is per process: several worker processes each
reserve their own blocks, so their keys are unique but interleave, and
unused keys of a block are gaps when the process stops.
"""

import asyncio

COUNTERS = "siderite_counters"
DEFAULT_BLOCK_SIZE = 64


class KeyBlockAllocator:
    """Hands out unique integer keys for one collection, a block at a time.

    Taking a key from the current block does not await, so concurrent
    asyncio tasks cannot interleave inside it. When the block is exhausted,
    one task reserves a new block under an ``asyncio.Lock`` while the
    others wait and then re-check the block.
    """

    def __init__(self, counters, collection, block_size=DEFAULT_BLOCK_SIZE):
        """Create an allocator with no reserved keys.

        Args:
            counters: Async collection (e.g. Motor) holding the counters.
            collection: Name of the collection whose keys are allocated;
                the ``_id`` of its counter document.
            block_size: Keys reserved per counter round trip (at least 1).
        """
        self._counters = counters
        self._collection = collection
        self._block_size = max(1, int(block_size))
        self._next = 0
        self._end = 0  # Exclusive end of the reserved block.
        self._refill = asyncio.Lock()

    def _take(self):
        """Take the next key of the current block without awaiting.

        Returns:
            The key, or None when the block is exhausted.
        """
        if self._next >= self._end:
            return None
        key = self._next
        self._next += 1
        return key

    async def next_key(self):
        """Return the next key, unique among allocators sharing the counter.

        Returns:
            An integer key from a block reserved by this allocator.
        """
        key = self._take()
        if key is not None:
            return key
        async with self._refill:
            # Another task may have refilled while this one waited.
            key = self._take()
            if key is not None:
                return key
            last = await self._reserve_block()
            self._next = last - self._block_size + 1
            self._end = last + 1
            return self._take()

    async def _reserve_block(self):
        """Reserve ``block_size`` keys with one atomic counter increment.

        Returns:
            The new ``seq``: the last key of the reserved block.
        """
        counter = await self._counters.find_one_and_update(
            {"_id": self._collection},
            {"$inc": {"seq": self._block_size}},
            upsert=True,
            # ReturnDocument.AFTER, without importing pymongo here.
            return_document=True,
        )
        return int(counter["seq"])
