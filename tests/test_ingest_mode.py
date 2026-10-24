"""Run with: DATABASE_URL=x python -m unittest tests/test_ingest_mode.py (from the repo root)."""
import contextlib
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))
os.environ.setdefault("DATABASE_URL", "unused")

import common  # noqa: E402


class FakeConn:
    def __init__(self):
        self.calls = []

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=None):
        self.calls.append((sql, params))


class IngestModeTest(unittest.TestCase):
    def setUp(self):
        self.saved = common.STATE["insert_only"]
        self.addCleanup(common.STATE.__setitem__, "insert_only", self.saved)

    def last_sql(self, conn):
        return conn.calls[-1][0]

    def test_default_is_upsert(self):
        common.STATE["insert_only"] = False
        conn = FakeConn()
        common._ingest_call(conn, "run-1", "[]", None)
        self.assertIn("prospecting.bulk_ingest(", self.last_sql(conn))
        self.assertNotIn("new_only", self.last_sql(conn))

    def test_insert_only_uses_new_only_function(self):
        common.STATE["insert_only"] = True
        conn = FakeConn()
        common._ingest_call(conn, "run-1", "[]", "note")
        self.assertIn("prospecting.bulk_ingest_new_only(", self.last_sql(conn))
        self.assertEqual(conn.calls[-1][1], ("run-1", "[]", "note"))

    def test_run_job_sets_mode_and_other_jobs_stay_upsert(self):
        seen = []
        orig = common.ingest
        common.ingest = lambda source, records, note=None, skip=None, insert_only=None: (
            seen.append(common.STATE["insert_only"]) or (0, True))
        self.addCleanup(setattr, common, "ingest", orig)
        common.run_job("x", lambda: iter(()), insert_only=True)
        common.run_job("y", lambda: iter(()))
        self.assertEqual(seen, [True, False])


if __name__ == "__main__":
    unittest.main()
