"""Run with: DATABASE_URL=x python -m unittest tests/test_florida_quarterly.py (from the repo root)."""
import os
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))
os.environ.setdefault("DATABASE_URL", "unused")

import florida_quarterly as fq  # noqa: E402


def line(doc):
    return f"{doc:<12}" + "A" * 1100


class RecordsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        fq.LOCAL = os.path.join(self.dir.name, "cordata.zip")
        with zipfile.ZipFile(fq.LOCAL, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("cordata0.txt", "\n".join(line(f"A{i:05d}") for i in range(50)) + "\n")
            z.writestr("cordata[1].txt", "\n".join(line(f"B{i:05d}") for i in range(30)) + "\n")
            z.writestr("cordata2.txt", "\n".join(line(f"C{i:05d}") for i in range(20)) + "\n")
        self.native = fq.NATIVE_METHODS
        self.addCleanup(setattr, fq, "NATIVE_METHODS", self.native)

    def test_zipfile_path_reads_every_member(self):
        self.assertEqual(len(list(fq.records())), 100)

    def test_unzip_path_reads_every_member_and_survives_member_boundaries(self):
        # Regression: the TextIOWrapper closes the unzip pipe when its loop ends, and the
        # cleanup then read the closed pipe ("ValueError: read of closed file").
        fq.NATIVE_METHODS = ()  # force every member through `unzip -p`
        keys = [k for k, _, _ in fq.records()]
        self.assertEqual(len(keys), 100)
        self.assertEqual(keys[0], "A00000")
        self.assertEqual(keys[-1], "B00029")  # members are read in sorted name order

    def test_unzip_path_stopping_early_does_not_hang_or_raise_from_cleanup(self):
        fq.NATIVE_METHODS = ()
        gen = fq.records()
        for _ in range(5):
            next(gen)
        gen.close()  # what happens when the ingest loop hands over at MAX_MINUTES

    def test_unzip_failure_is_raised(self):
        fq.NATIVE_METHODS = ()
        with open(fq.LOCAL, "r+b") as f:  # corrupt the compressed data of the first member
            f.seek(60)
            f.write(b"\xff" * 40)
        with self.assertRaises(RuntimeError):
            list(fq.records())


if __name__ == "__main__":
    unittest.main()
