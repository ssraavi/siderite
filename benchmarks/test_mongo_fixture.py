"""A MongoDB reseed never hands out keys an app still holds in memory."""

import sys
import types
import unittest
from unittest.mock import patch

import database_fixtures


class FakeCollection:
    """In-memory collection with the calls the Mongo fixture makes."""

    def __init__(self):
        """Create an empty collection."""
        self.docs = {}

    async def drop(self):
        """Remove every document."""
        self.docs.clear()

    async def delete_many(self, query):
        """Remove every document; the fixture only passes ``{}``.

        Args:
            query: Must be empty.
        """
        assert query == {}
        self.docs.clear()

    async def insert_many(self, docs):
        """Insert documents keyed by ``_id``.

        Args:
            docs: Documents to insert.
        """
        for doc in docs:
            assert doc["_id"] not in self.docs
            self.docs[doc["_id"]] = dict(doc)

    async def update_one(self, query, update, upsert=False):
        """Apply a ``$max`` update, inserting the document when allowed.

        Args:
            query: ``{_id: name}`` filter.
            update: ``{$max: {field: value}}`` update.
            upsert: Whether a missing document is created.
        """
        doc = self.docs.get(query["_id"])
        if doc is None:
            if not upsert:
                return
            doc = self.docs[query["_id"]] = dict(query)
        for field, value in update["$max"].items():
            doc[field] = max(doc.get(field, value), value)


class FakeClient:
    """Motor client stand-in sharing one in-memory database."""

    database = {}

    def __init__(self, url):
        """Accept the URL like ``AsyncIOMotorClient``.

        Args:
            url: Ignored connection string.
        """
        self.url = url

    def get_default_database(self, name):
        """Return the shared database.

        Args:
            name: Ignored default database name.

        Returns:
            Object whose items are collections.
        """
        return self

    def __getitem__(self, name):
        """Return (creating) the named collection.

        Args:
            name: Collection name.

        Returns:
            The fake collection.
        """
        return self.database.setdefault(name, FakeCollection())

    def close(self):
        """Close nothing."""


class MongoReseedTest(unittest.TestCase):
    """The counter is reset only when the fixture is recreated."""

    def setUp(self):
        """Install the fake Motor module for ``seed_database``."""
        FakeClient.database = {}
        motor = types.ModuleType("motor")
        motor_asyncio = types.ModuleType("motor.motor_asyncio")
        motor_asyncio.AsyncIOMotorClient = FakeClient
        motor.motor_asyncio = motor_asyncio
        modules = patch.dict(
            sys.modules,
            {"motor": motor, "motor.motor_asyncio": motor_asyncio},
        )
        modules.start()
        self.addCleanup(modules.stop)

    def seed(self, recreate=False):
        """Seed the fake database.

        Args:
            recreate: Whether the fixture is created from scratch.
        """
        database_fixtures.seed_database(
            "mongodb", recreate=recreate, url="mongodb://fake/siderite"
        )

    def counter(self):
        """Return the ``todos`` counter value."""
        return FakeClient.database["siderite_counters"].docs["todos"]["seq"]

    def test_recreate_starts_counter_at_fixture_size(self):
        """A fresh fixture holds rows 1..100 and the counter at 100."""
        self.seed(recreate=True)
        self.assertEqual(self.counter(), 100)
        self.assertEqual(
            sorted(FakeClient.database["todos"].docs), list(range(1, 101))
        )

    def test_reseed_keeps_reserved_keys_reserved(self):
        """A reseed after reservations never lowers the counter."""
        self.seed(recreate=True)
        # An app reserved eight blocks of 64 during warm-up and still
        # holds part of the last one in memory.
        FakeClient.database["siderite_counters"].docs["todos"]["seq"] = 612
        FakeClient.database["todos"].docs[612] = {"_id": 612}
        self.seed()
        self.assertEqual(self.counter(), 612)
        self.assertEqual(
            sorted(FakeClient.database["todos"].docs), list(range(1, 101))
        )

    def test_reseed_creates_missing_counter(self):
        """A reseed without a counter document creates it at 100."""
        self.seed(recreate=True)
        FakeClient.database["siderite_counters"].docs.clear()
        self.seed()
        self.assertEqual(self.counter(), 100)

    def test_recreate_resets_counter(self):
        """Recreating the fixture resets a raised counter to 100."""
        self.seed(recreate=True)
        FakeClient.database["siderite_counters"].docs["todos"]["seq"] = 612
        self.seed(recreate=True)
        self.assertEqual(self.counter(), 100)


if __name__ == "__main__":
    unittest.main()
