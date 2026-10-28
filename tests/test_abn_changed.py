"""ABN changed-only job. Run with: DATABASE_URL=x python -m unittest discover -s tests (from the repo root)."""
import contextlib
import os
import sys
import tempfile
import unittest
import zipfile
from datetime import date
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))
os.environ.setdefault("DATABASE_URL", "unused")

import abn_bulk  # noqa: E402
import common  # noqa: E402

TODAY = date(2026, 10, 28)


def abr(abn, status="ACT", etype="PRV", rlud="20261001", frm="20200101", name="Acme"):
    attrs = f' recordLastUpdatedDate="{rlud}"' if rlud is not None else ""
    frm_attr = f' ABNStatusFromDate="{frm}"' if frm is not None else ""
    return (f'<ABR{attrs} replaced="N"><ABN status="{status}"{frm_attr}>{abn}</ABN>'
            f'<EntityType><EntityTypeInd>{etype}</EntityTypeInd></EntityType>'
            f'<MainEntity><NonIndividualName><NonIndividualNameText>{name}</NonIndividualNameText></NonIndividualName></MainEntity></ABR>')


def make_zip(tmp, rows, name="a.zip"):
    path = os.path.join(tmp, name)
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("part1.xml", "<Transfer>" + "".join(rows) + "</Transfer>")
    return path


# file order mixes active and cancelled so the stream reorder is visible
ROWS = [
    abr("1", "ACT"),
    abr("2", "CAN", rlud="20261005", frm="20261005"),            # recent cancelled: kept
    abr("3", "CAN", rlud="20200101", frm="20200101"),            # old cancelled: dropped
    abr("4", "ACT", etype="IND"),                                # individual: dropped
    abr("5", "CAN", rlud=None, frm=None),                        # no date: kept
    abr("6", "ACT"),
    abr("7", "CAN", rlud="20261001", frm="20200101"),            # greatest() of the two is recent: kept
    abr("8", "CAN", etype="IND", rlud="20261005"),               # cancelled individual: dropped
    abr("9", "CAN", rlud="21240101", frm="20200101"),            # typo year ignored, old date: dropped
    abr("10", "ACT", rlud="20260230"),                           # impossible date: still ACT, sent
]


class FakeCur:
    def __init__(self, v):
        self.v = v

    def fetchone(self):
        return (self.v,)


class FakeConn:
    def __init__(self, result=None):
        self.calls = []
        self.result = result

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return FakeCur(self.result)

    def close(self):
        pass


class Base(unittest.TestCase):
    KEYS = ("insert_only", "changed", "chain", "changed_same", "changed_newer", "changed_total", "new_total",
            "durations", "run", "sent", "skip", "handed_over")

    def setUp(self):
        self.saved = {k: common.STATE[k] for k in self.KEYS}
        common.STATE.update(insert_only=False, changed=False, chain={}, changed_same=0, changed_newer=0,
                            changed_total=0, new_total=0, durations=[], run=None, sent=0, skip=0, handed_over=False)
        self.addCleanup(common.STATE.update, self.saved)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)


class ModeTest(Base):
    def test_changed_mode_uses_changed_function_with_chain_counter(self):
        common.STATE["changed"] = True
        common.STATE["changed_same"] = 7
        conn = FakeConn({"changed_same": 2, "changed_newer": 3, "changed": 5, "new_jobs": 1})
        common._ingest_call(conn, "run-1", "[]", "n")
        sql, params = conn.calls[-1]
        self.assertIn("prospecting.bulk_ingest_changed(", sql)
        self.assertEqual(params, ("run-1", "[]", "n", 7, common.MAX_CHANGED_SAME))
        self.assertEqual(common.STATE["changed_same"], 9)  # running total
        self.assertEqual(common.STATE["changed_total"], 5)

    def test_changed_wins_over_insert_only_and_default_is_unchanged(self):
        common.STATE.update(changed=True, insert_only=True)
        self.assertIn("bulk_ingest_changed(", common._ingest_sql())
        common.STATE.update(changed=False, insert_only=True)
        self.assertIn("bulk_ingest_new_only(", common._ingest_sql())
        common.STATE.update(changed=False, insert_only=False)
        self.assertIn("bulk_ingest(", common._ingest_sql())

    def test_same_guard_in_result_stops_the_job(self):
        common.STATE["changed"] = True
        conn = FakeConn({"changed_same": 5, "guard": True})
        with self.assertRaises(common.GuardError) as cm:
            common._ingest_call(conn, "run-1", "[]", None)
        self.assertTrue(cm.exception.note.startswith("same_rlud_guard:"))

    def test_guard_note_is_written_as_is_with_resume_point(self):
        common.STATE.update(changed=True, run="run-1", sent=10, skip=5,
                            chain={"cutoff": "2026-09-14"})
        conn = FakeConn()
        with mock.patch.object(common, "_connect", lambda: conn):
            def boom():
                raise common.GuardError("same_rlud_guard: x")
            with self.assertRaises(common.GuardError):
                common.guarded("src", boom)
        note = conn.calls[-1][1][1]
        self.assertTrue(note.startswith("same_rlud_guard: x"))
        self.assertIn("skip=15 cutoff=2026-09-14", note)

    def test_error_note_without_chain_keeps_old_text(self):
        common.STATE.update(run="run-1", sent=1, skip=2)
        conn = FakeConn()
        with mock.patch.object(common, "_connect", lambda: conn):
            with self.assertRaises(ValueError):
                common.guarded("src", lambda: (_ for _ in ()).throw(ValueError("x")))
        self.assertTrue(conn.calls[-1][1][1].endswith("(resume with skip=3)"))


class ContinuationTest(Base):
    def dispatch(self, fn):
        env = {"GITHUB_REPOSITORY": "o/r", "GITHUB_WORKFLOW_REF": "o/r/.github/workflows/abn-bulk.yml@refs/heads/main",
               "GITHUB_TOKEN": "t"}
        with mock.patch.dict(os.environ, env), mock.patch("requests.post") as post:
            fn()
        return post.call_args.kwargs["json"]["inputs"]

    def test_dispatch_carries_cutoff_and_counters(self):
        common.STATE["chain"] = {"cutoff": "2026-09-14", "max_nonact": 500000, "nonact_count": 12, "chain_date": "2026-10-28"}
        common.STATE["changed_same"] = 33
        inputs = self.dispatch(lambda: common._dispatch_continuation(400, "src"))
        self.assertEqual(inputs["cutoff"], "2026-09-14")
        self.assertEqual(inputs["nonact_count"], "12")
        self.assertEqual(inputs["chain_date"], "2026-10-28")
        self.assertEqual(inputs["changed_same"], "33")
        self.assertEqual(inputs["skip"], "400")

    def test_other_jobs_send_only_the_old_inputs(self):
        inputs = self.dispatch(lambda: common._dispatch_continuation(400, "src"))
        self.assertEqual(set(inputs), {"skip", "record_limit", "source_id"})

    def test_run_job_continuation_carries_cutoff(self):
        common.STATE["chain"] = {"cutoff": "2026-09-14"}
        with mock.patch.object(common, "ingest", lambda *a, **k: (50, False)):
            inputs = self.dispatch(lambda: common.run_job("x", lambda: iter(()), changed=True))
        self.assertEqual(inputs["cutoff"], "2026-09-14")
        self.assertEqual(inputs["skip"], "50")

    def test_watchdog_path_carries_cutoff_and_notes_it(self):
        common.STATE.update(run="run-1", sent=7, skip=3, chain={"cutoff": "2026-09-14"}, changed=True)
        conn = FakeConn()
        exits = []
        env = {"GITHUB_REPOSITORY": "o/r", "GITHUB_WORKFLOW_REF": "o/r/.github/workflows/abn-bulk.yml@x", "GITHUB_TOKEN": "t"}
        with mock.patch.dict(os.environ, env), mock.patch("requests.post") as post, \
                mock.patch.object(common, "_connect", lambda: conn), mock.patch.object(common, "WATCHDOG_GRACE_MIN", -1), \
                mock.patch.object(common, "MAX_MINUTES", 0), mock.patch.object(common.os, "_exit", exits.append):
            common._watchdog()
        self.assertEqual(post.call_args.kwargs["json"]["inputs"]["cutoff"], "2026-09-14")
        self.assertEqual(post.call_args.kwargs["json"]["inputs"]["skip"], "10")
        self.assertEqual(conn.calls[-1][1][2], "stalled: continue from skip=10 cutoff=2026-09-14 changed_same=0")
        self.assertEqual(exits, [3])


class MarkerTest(Base):
    def run_it(self, finished, limit=0, on_complete=lambda: "complete: file=f max_rlud=20261001", changed=True):
        notes = []
        with mock.patch.object(common, "ingest", lambda *a, **k: (10, finished)), \
                mock.patch.object(common, "_write_note", notes.append), \
                mock.patch.object(common, "LIMIT", limit), \
                mock.patch.object(common, "_dispatch_continuation", lambda *a: notes.append("continuation")):
            common.run_job("x", lambda: iter(()), changed=changed, on_complete=on_complete)
        return notes

    def test_marker_only_on_full_finish(self):
        self.assertEqual(self.run_it(True), ["complete: file=f max_rlud=20261001"])
        self.assertEqual(self.run_it(False), ["continuation"])  # paused: no marker

    def test_limited_run_writes_test_note_not_marker(self):
        self.assertEqual(self.run_it(True, limit=2000), ["test: record_limit=2000"])

    def test_other_jobs_write_nothing(self):
        self.assertEqual(self.run_it(True, changed=False), [])


class RecordsTest(Base):
    def paths(self, rows=ROWS):
        return [make_zip(self.tmp.name, rows)]

    def test_buckets_and_order(self):
        stats = abn_bulk.Stats()
        out = list(abn_bulk.records(self.paths(), "2026-09-14", 100, TODAY, stats=stats))
        keys = [k for k, _, _ in out]
        # ACT in file order (1, 6, 10), then kept cancelled in file order (2, 5, 7)
        self.assertEqual(keys, ["1", "6", "10", "2", "5", "7"])
        self.assertEqual((stats.act, stats.nonact_sent, stats.nonact_old, stats.ind), (3, 3, 2, 2))
        self.assertEqual(stats.nonact_total, 5)
        self.assertEqual(stats.max_rlud, "20261005")
        self.assertEqual(out[0][2]["ABN"]["status"], "ACT")
        self.assertEqual(out[3][2]["ABN"]["status"], "CAN")
        self.assertEqual(out[0][2]["recordLastUpdatedDate"], "20261001")

    def test_s2_count(self):
        stats = abn_bulk.count_pass(self.paths(), "2026-09-14", TODAY)
        # row 7: rlud 20261001 > from 20200101 so not S2; row 2 equal; none has rlud < from
        self.assertEqual(stats.s2, 0)
        rows = [abr("1", "CAN", rlud="20200101", frm="20261001")]
        self.assertEqual(abn_bulk.count_pass(self.paths(rows), "2026-09-14", TODAY).s2, 1)

    def test_all_cutoff_sends_every_cancelled_non_individual(self):
        stats = abn_bulk.count_pass(self.paths(), "all", TODAY)
        self.assertEqual(stats.nonact_sent, 5)

    def test_volume_guard_trips_at_max_plus_one_before_any_cancelled_record(self):
        sent = []
        gen = abn_bulk.records(self.paths(), "2026-09-14", 2, TODAY)  # 3 cancelled kept, max 2
        with self.assertRaises(common.GuardError) as cm:
            for r in gen:
                sent.append(r[0])
        self.assertEqual(sent, ["1", "6", "10"])  # only active ones, no cancelled record was yielded
        self.assertEqual(cm.exception.note, "nonact_guard: cutoff=2026-09-14 nonact=3 max=2 rerun with max_nonact>=3 or another cutoff")
        # exactly at the limit passes
        self.assertEqual(len(list(abn_bulk.records(self.paths(), "2026-09-14", 3, TODAY))), 6)

    def test_precount_guard_logic_uses_the_same_count(self):
        self.assertEqual(abn_bulk.count_pass(self.paths(), "2026-09-14", TODAY).nonact_sent, 3)

    def test_continuation_recounts_and_fails_on_difference(self):
        with self.assertRaises(common.GuardError) as cm:
            list(abn_bulk.records(self.paths(), "2026-09-14", 100, TODAY, expected_nonact=2))
        self.assertIn("count changed", cm.exception.note)
        # same count: fine
        self.assertEqual(len(list(abn_bulk.records(self.paths(), "2026-09-14", 100, TODAY, expected_nonact=3))), 6)

    def test_record_at_index_k_is_identical_in_two_fresh_calls_in_the_tail(self):
        a = list(abn_bulk.records(self.paths(), "2026-09-14", 100, TODAY))
        b = list(abn_bulk.records(self.paths(), "2026-09-14", 100, TODAY))
        self.assertEqual(a, b)
        for k in (3, 4, 5):  # the non-ACT tail
            self.assertEqual(a[k], b[k])
            self.assertEqual(a[k][2]["ABN"]["status"], "CAN")

    def test_queue_file_removed(self):
        with mock.patch.dict(os.environ, {"RUNNER_TEMP": self.tmp.name}):
            list(abn_bulk.records(self.paths(), "2026-09-14", 100, TODAY))
        self.assertEqual([f for f in os.listdir(self.tmp.name) if f.startswith("abn_nonact_")], [])

    def test_valid_ymd(self):
        for bad in (None, "", "2026", "20260230", "21240101", "99999999", "2026-10-1", "abcdefgh", "00000101"):
            self.assertIsNone(abn_bulk.valid_ymd(bad, TODAY), bad)
        self.assertEqual(abn_bulk.valid_ymd("20261029", TODAY), "20261029")  # today + 1 allowed
        self.assertIsNone(abn_bulk.valid_ymd("20261030", TODAY))


class CountOnlyTest(Base):
    def test_count_only_opens_no_database_connection(self):
        path = make_zip(self.tmp.name, ROWS)
        env = {"COUNT_ONLY": "1", "CUTOFF": "2026-09-14"}
        with mock.patch.dict(os.environ, env), \
                mock.patch.object(abn_bulk, "zip_resources", lambda: [{"url": "u", "last_modified": "2026-10-04T10:00:00"}]), \
                mock.patch.object(abn_bulk, "download", lambda url, p: __import__("shutil").copy(path, p)), \
                mock.patch.object(common, "_connect", side_effect=AssertionError("database opened")), \
                mock.patch.object(common, "run_job", side_effect=AssertionError("run_job called")), \
                mock.patch.object(abn_bulk, "run_job", side_effect=AssertionError("run_job called")):
            abn_bulk.main()

    def test_first_run_guard_trips_in_the_precount_before_any_database_call(self):
        path = make_zip(self.tmp.name, ROWS)
        env = {"CUTOFF": "2026-09-14", "MAX_NONACT": "2", "COUNT_ONLY": ""}
        with mock.patch.dict(os.environ, env), mock.patch.object(common, "SKIP", 0), \
                mock.patch.object(abn_bulk, "zip_resources", lambda: [{"url": "u", "last_modified": "2026-10-04T10:00:00"}]), \
                mock.patch.object(abn_bulk, "download", lambda url, p: __import__("shutil").copy(path, p)), \
                mock.patch.object(common, "_connect", side_effect=AssertionError("database opened")):
            with self.assertRaises(common.GuardError) as cm:
                abn_bulk.main()
        self.assertTrue(cm.exception.note.startswith("nonact_guard: cutoff=2026-09-14 nonact=3 max=2"))

    def test_continuation_without_cutoff_fails(self):
        path = make_zip(self.tmp.name, ROWS)
        env = {"CUTOFF": "", "CHAIN_DATE": ""}
        with mock.patch.dict(os.environ, env), mock.patch.object(common, "SKIP", 100), \
                mock.patch.object(abn_bulk, "zip_resources", lambda: [{"url": "u", "last_modified": "2026-10-04T10:00:00"}]), \
                mock.patch.object(abn_bulk, "download", lambda url, p: __import__("shutil").copy(path, p)), \
                mock.patch.object(common, "_connect", side_effect=AssertionError("database opened")):
            with self.assertRaises(common.GuardError) as cm:
                abn_bulk.main()
        self.assertIn("without cutoff", cm.exception.note)


CHAIN = {"cutoff": "2026-09-14", "chain_date": "2026-10-10", "nonact_count": 4, "max_nonact": 500000}


class ResumeAndErrorTest(Base):
    def test_resume_hint_lists_every_chain_value(self):
        common.STATE.update(chain=dict(CHAIN), changed_same=40)
        self.assertEqual(common._resume_hint(),
                         " cutoff=2026-09-14 chain_date=2026-10-10 nonact_count=4 max_nonact=500000 changed_same=40")
        common.STATE["chain"] = {}
        self.assertEqual(common._resume_hint(), "")

    def test_paused_note_carries_cutoff_and_the_other_chain_values(self):
        common.STATE.update(changed=True, chain=dict(CHAIN), changed_same=7)
        conn = FakeConn({"changed": 0})
        with mock.patch.object(common, "_connect", lambda: conn), mock.patch.object(common.threading, "Thread"), \
                mock.patch.object(common, "MAX_MINUTES", -1):
            sent, finished = common.ingest("src", iter([("k", None, {"a": 1})]), skip=0)
        self.assertFalse(finished)
        note = conn.calls[-1][1][2]
        self.assertEqual(note, "paused: continue from skip=0 cutoff=2026-09-14 chain_date=2026-10-10 "
                               "nonact_count=4 max_nonact=500000 changed_same=7")

    def test_guard_error_before_any_run_exists_starts_a_run_and_writes_the_note_as_is(self):
        common.STATE.update(changed=True, run=None, chain=dict(CHAIN))
        conn = FakeConn("run-9")
        with mock.patch.object(common, "_connect", lambda: conn):
            with self.assertRaises(common.GuardError):
                common.guarded("src", lambda: (_ for _ in ()).throw(common.GuardError("nonact_guard: too many")))
        self.assertIn("bulk_run_start", conn.calls[0][0])
        self.assertEqual(conn.calls[1][1], ("run-9", "nonact_guard: too many"))

    def test_count_only_error_reraises_without_connecting(self):
        with mock.patch.dict(os.environ, {"COUNT_ONLY": "1"}), \
                mock.patch.object(common, "_connect") as connect:
            with self.assertRaises(ValueError):
                common.guarded("src", lambda: (_ for _ in ()).throw(ValueError("bad file")))
        connect.assert_not_called()


class MainChainTest(Base):
    """abn_bulk.main() on continuations, with the file and the job runner replaced."""

    def run_main(self, env, skip, source_id_old="", ids="2026-10-04T10:00:00"):
        path = make_zip(self.tmp.name, ROWS)
        seen = {}

        def fake_run_job(source, records_fn, **kw):
            records_fn()
            seen["kw"] = kw

        def fake_records(paths, cutoff, max_nonact, today, stats=None, expected_nonact=None):
            seen.update(paths=paths, cutoff=cutoff, today=today, expected=expected_nonact, max_nonact=max_nonact)
            return iter(())

        base = {"CUTOFF": "", "CHAIN_DATE": "", "NONACT_COUNT": "", "CHANGED_SAME": "", "MAX_NONACT": "", "COUNT_ONLY": ""}
        base.update(env)
        with mock.patch.dict(os.environ, base), mock.patch.object(common, "SKIP", skip), \
                mock.patch.object(common, "SOURCE_ID", source_id_old), \
                mock.patch.object(abn_bulk, "zip_resources", lambda: [{"url": "u", "last_modified": ids}]), \
                mock.patch.object(abn_bulk, "download", lambda url, p: __import__("shutil").copy(path, p)), \
                mock.patch.object(abn_bulk, "run_job", fake_run_job), mock.patch.object(abn_bulk, "records", fake_records), \
                mock.patch.object(abn_bulk, "resolve_cutoff", side_effect=AssertionError("cutoff recomputed")), \
                mock.patch.object(common, "_connect", side_effect=AssertionError("database opened")):
            abn_bulk.main()
        return seen

    def test_continuation_uses_chain_date_as_today_and_keeps_the_chain_values(self):
        env = {"CUTOFF": "2026-09-14", "CHAIN_DATE": "2026-10-10", "NONACT_COUNT": "4", "CHANGED_SAME": "9"}
        seen = self.run_main(env, 100)
        self.assertEqual(seen["today"], date(2026, 10, 10))
        self.assertEqual(seen["cutoff"], "2026-09-14")
        self.assertEqual(seen["expected"], 4)
        self.assertEqual(common.STATE["chain"], {"cutoff": "2026-09-14", "max_nonact": 500000, "nonact_count": 4,
                                                 "chain_date": "2026-10-10"})
        self.assertEqual(common.STATE["changed_same"], 9)

    def test_continuation_without_nonact_count_fails(self):
        env = {"CUTOFF": "2026-09-14", "CHAIN_DATE": "2026-10-10"}
        with self.assertRaises(common.GuardError) as cm:
            self.run_main(env, 100)
        self.assertIn("without nonact_count", cm.exception.note)

    def test_negative_chain_inputs_are_rejected(self):
        for name in ("CHANGED_SAME", "MAX_NONACT", "NONACT_COUNT"):
            env = {"CUTOFF": "2026-09-14", "CHAIN_DATE": "2026-10-10", "NONACT_COUNT": "4", name: "-1"}
            with self.assertRaises(common.GuardError, msg=name) as cm:
                self.run_main(env, 100)
            self.assertIn("negative", cm.exception.note)

    def test_restart_on_a_changed_file_recounts_and_keeps_the_cutoff(self):
        # SKIP > 0 but the chain's file id differs from the new file: effective skip 0, so the new file is counted again
        env = {"CUTOFF": "2026-09-14", "CHAIN_DATE": "2026-10-10", "NONACT_COUNT": "999", "CHANGED_SAME": "3"}
        seen = self.run_main(env, 100, source_id_old="old-file-id")
        self.assertEqual(seen["cutoff"], "2026-09-14")            # kept, not recomputed
        self.assertEqual(seen["today"], date(2026, 10, 10))
        self.assertEqual(seen["expected"], 3)                     # recounted from the new file (rows 2, 5, 7), not 999
        self.assertEqual(common.STATE["chain"]["nonact_count"], 3)
        self.assertEqual(seen["kw"]["source_id"], "2026-10-04T10:00:00")
        self.assertNotIn("/tmp/abn_0.zip", seen["paths"])


if __name__ == "__main__":
    unittest.main()
