"""ABN Lookup bulk extract (data.gov.au) -> 'Australia businesses ABN (auto)'.

Keeps non-individual ABNs. Each <ABR> element is converted to a dict mechanically.

Changed-only (spec e026): the register has ~4.7M rows and only a few thousand are new or changed each
month. Every active record is sent; the database inserts new keys and updates an existing row only when
the record changed (recordLastUpdatedDate newer, or the same date with a different item), so unchanged
rows are not rewritten (re-writing all of them every month produced ~4 GB of dead rows).
Cancelled (non-ACT) records are sent too when their date is on or after a cutoff; the database only
updates rows that already exist for them, it never inserts them.

Stream order is fixed: active records in file order, then the cancelled records that pass the cutoff,
in file order (spooled to a queue file while the active ones stream). Every run re-parses the whole file with
the same cutoff, so the record at stream index k is the same in every run and skip counts line up.
"""
import json
import os
import tempfile
import time
import zipfile
from datetime import date, timedelta

import requests
from lxml import etree

import common
from common import GuardError, clean, guarded, log, run_job

# Used when no earlier complete run exists: 14 days before the newest rlud in the first full load (20260928).
CUTOFF_FLOOR = "2026-09-14"

PACKAGE = "https://data.gov.au/data/api/3/action/package_show?id=abn-bulk-extract"
KEEP_INDIVIDUALS = os.environ.get("KEEP_INDIVIDUALS") == "1"


def zip_resources():
    res = requests.get(PACKAGE, timeout=60).json()["result"]["resources"]
    # keep the API's order: continuation skip counts depend on it
    return [r for r in res if (r.get("format") or "").upper() == "ZIP"]


def download(url, path):
    log(f"downloading {url}")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)


def to_dict(el):
    out = dict(el.attrib)
    children = list(el)
    if not children:
        text = (el.text or "").strip()
        if out:
            if text:
                out["text"] = text
            return out
        return text
    for c in children:
        v = to_dict(c)
        if c.tag in out:
            if not isinstance(out[c.tag], list):
                out[c.tag] = [out[c.tag]]
            out[c.tag].append(v)
        else:
            out[c.tag] = v
    return out


def yyyymmdd(v):
    return f"{v[0:4]}-{v[4:6]}-{v[6:8]}" if v and len(v) == 8 and v.isdigit() else None


def valid_ymd(v, today):
    """v if it is a real YYYYMMDD date no later than today + 1 day, else None (same rule as the database).
    `today` is a date."""
    if not v or len(v) != 8 or not v.isascii() or not v.isdigit():
        return None
    try:
        d = date(int(v[0:4]), int(v[4:6]), int(v[6:8]))
    except ValueError:
        return None
    return v if d <= today + timedelta(days=1) else None


def parse_cutoff(v):
    """'YYYY-MM-DD' -> 'YYYYMMDD'; 'all' -> '00000000' (every cancelled record is sent)."""
    if v == "all":
        return "00000000"
    return date.fromisoformat(v).strftime("%Y%m%d")


class Stats:
    def __init__(self):
        self.total = self.act = self.nonact_total = self.nonact_sent = self.nonact_old = 0
        self.ind = self.nokey = self.s2 = 0
        self.top = []  # the 3 highest distinct valid rlud values

    @property
    def max_rlud(self):
        return self.top[0] if self.top else None

    def note_rlud(self, v):
        if v and v not in self.top:
            self.top = sorted(self.top + [v], reverse=True)[:3]

    def summary(self):
        return (f"records {self.total:,}: ACT {self.act:,}, non-ACT total {self.nonact_total:,}, "
                f"non-ACT sent (>= cutoff or no date) {self.nonact_sent:,}, non-ACT dropped (older) {self.nonact_old:,}, "
                f"IND skipped {self.ind:,}, no ABN {self.nokey:,}, S2 (rlud < status date) {self.s2:,}; "
                f"top rlud {self.top}")


def scan(paths, cut, today, stats, build):
    """Walk every <ABR> in file order. Yields (bucket, abn, status_from, item) for records to send:
    bucket 'act' or 'nonact' (a cancelled record on or after the cutoff, or without a usable date).
    Everything else is only counted. item is None unless build is True (the count-only pass skips the cost)."""
    for path in paths:
        with zipfile.ZipFile(path) as z:
            for name in sorted(z.namelist()):
                if not name.lower().endswith(".xml"):
                    continue
                log(f"reading {name}")
                with z.open(name) as f:
                    for _, el in etree.iterparse(f, tag="ABR"):
                        stats.total += 1
                        abn_el = el.find("ABN")
                        etype = el.findtext("EntityType/EntityTypeInd")
                        status = abn_el.get("status") if abn_el is not None else None
                        abn = (abn_el.text or "").strip() if abn_el is not None else ""
                        rlud = valid_ymd(el.get("recordLastUpdatedDate"), today)
                        frm = valid_ymd(abn_el.get("ABNStatusFromDate"), today) if abn_el is not None else None
                        stats.note_rlud(rlud)
                        bucket = None
                        if etype == "IND" and not KEEP_INDIVIDUALS:
                            stats.ind += 1
                        elif not abn:
                            stats.nokey += 1
                        elif status == "ACT":
                            stats.act += 1
                            bucket = "act"
                        else:
                            stats.nonact_total += 1
                            if rlud and frm and rlud < frm:
                                stats.s2 += 1
                            dates = [d for d in (rlud, frm) if d]
                            if not dates or max(dates) >= cut:
                                stats.nonact_sent += 1
                                bucket = "nonact"
                            else:
                                stats.nonact_old += 1
                        if bucket:
                            yield bucket, abn, frm, (clean(to_dict(el)) if build else None)
                        el.clear()
                        while el.getprevious() is not None:
                            del el.getparent()[0]


def nonact_guard_note(cutoff, m, mx):
    return f"nonact_guard: cutoff={cutoff} nonact={m} max={mx} rerun with max_nonact>={m} or another cutoff"


def count_pass(paths, cutoff, today):
    """Parse the whole file and count; builds no items and touches no database."""
    stats = Stats()
    for _ in scan(paths, parse_cutoff(cutoff), today, stats, build=False):
        pass
    return stats


def records(paths, cutoff, max_nonact, today, stats=None, expected_nonact=None):
    """Stream (key, posted_at, item): active records in file order, then the queued cancelled ones in file order.
    Raises GuardError when the number of queued cancelled records is over max_nonact or differs from
    expected_nonact (the count run 1 saw); this happens when the active records are done, before any
    cancelled record is yielded."""
    stats = stats if stats is not None else Stats()
    queue = None
    qpath = None
    try:
        for bucket, abn, frm, item in scan(paths, parse_cutoff(cutoff), today, stats, build=True):
            if bucket == "act":
                yield abn, yyyymmdd(frm), item
            else:
                if queue is None:
                    fd, qpath = tempfile.mkstemp(prefix="abn_nonact_", suffix=".jsonl", dir=os.environ.get("RUNNER_TEMP") or None)
                    queue = os.fdopen(fd, "w", encoding="utf-8")
                queue.write(json.dumps([abn, yyyymmdd(frm), item], ensure_ascii=False) + "\n")
        if queue is not None:
            queue.close()
            queue = None
        log(f"file parsed. {stats.summary()}")
        if stats.nonact_sent > max_nonact:
            raise GuardError(nonact_guard_note(cutoff, stats.nonact_sent, max_nonact))
        if expected_nonact is not None and stats.nonact_sent != expected_nonact:
            raise GuardError(f"nonact_guard: cutoff={cutoff} non-ACT count changed during the chain "
                             f"(first run {expected_nonact}, now {stats.nonact_sent}); the file or the rules changed, restart the chain")
        if qpath:
            log(f"sending {stats.nonact_sent:,} cancelled records (queue file)")
            with open(qpath, encoding="utf-8") as f:
                for line in f:
                    abn, posted, item = json.loads(line)
                    yield abn, posted, item
    finally:
        if queue is not None:
            queue.close()
        if qpath:
            try:
                os.remove(qpath)
            except OSError:
                pass


def env_int(name, default):
    v = os.environ.get(name, "")
    return int(v) if v.strip() else default


def resolve_cutoff(source):
    """Cutoff for a new chain: the workflow input, else the date of the last complete run minus 14 days
    (prospecting.bulk_last_success), else the fixed floor."""
    given = os.environ.get("CUTOFF", "").strip()
    if given:
        parse_cutoff(given)  # validates
        return given
    conn = common._connect()
    try:
        d = conn.execute("select prospecting.bulk_last_success(%s)", (source,)).fetchone()[0]
    finally:
        conn.close()
    if d:
        log(f"cutoff {d} from the last complete run (minus 14 days)")
        return d.isoformat()
    log(f"no complete earlier run; cutoff is the fixed floor {CUTOFF_FLOOR}")
    return CUTOFF_FLOOR


def main():
    source = "Australia businesses ABN (auto)"
    paths, ids = [], []
    for i, res in enumerate(zip_resources()):
        p = f"/tmp/abn_{i}.zip"
        download(res["url"], p)
        paths.append(p)
        ids.append((res.get("last_modified") or res.get("metadata_modified") or res["url"])[:19])
    source_id = "|".join(ids)

    if os.environ.get("COUNT_ONLY", "").strip() in ("1", "true"):
        # No database connection at all: measures the volume before any real run.
        cutoff = os.environ.get("CUTOFF", "").strip() or CUTOFF_FLOOR
        parse_cutoff(cutoff)
        today = date.today()
        log(f"count only: cutoff {cutoff}, nothing is sent")
        log(count_pass(paths, cutoff, today).summary())
        return

    skip = common.effective_skip(source_id)
    max_nonact = env_int("MAX_NONACT", 500_000)
    if skip == 0 and not common.SKIP:
        cutoff = resolve_cutoff(source)
        today = date.today()
    else:
        # A continuation, or a restart on a changed file: the chain's own cutoff and date, never recomputed.
        cutoff = os.environ.get("CUTOFF", "").strip()
        chain_date = os.environ.get("CHAIN_DATE", "").strip()
        if not cutoff or not chain_date:
            raise GuardError("job error: continuation without cutoff or chain_date (the chain's own values are required)")
        parse_cutoff(cutoff)
        today = date.fromisoformat(chain_date)
    expected = None
    if skip == 0:
        t = time.time()
        stats = count_pass(paths, cutoff, today)
        log(f"count pass took {time.time() - t:.0f}s. {stats.summary()}")
        if stats.nonact_sent > max_nonact:
            raise GuardError(nonact_guard_note(cutoff, stats.nonact_sent, max_nonact))
        expected = stats.nonact_sent
    else:
        expected = env_int("NONACT_COUNT", -1)
        if expected < 0:
            raise GuardError("job error: continuation without nonact_count")
    common.STATE["chain"] = {"cutoff": cutoff, "max_nonact": max_nonact, "nonact_count": expected,
                             "chain_date": today.isoformat()}
    common.STATE["changed_same"] = env_int("CHANGED_SAME", 0)

    stream = Stats()
    file_tag = "_".join(source_id.split()) or "unknown"

    def on_complete():
        if stream.max_rlud is None:
            log("no valid recordLastUpdatedDate seen; no completion marker written")
            return None
        return f"complete: file={file_tag} max_rlud={stream.max_rlud}"

    run_job(source, lambda: records(paths, cutoff, max_nonact, today, stats=stream, expected_nonact=expected),
            source_id=source_id, changed=True, on_complete=on_complete)


if __name__ == "__main__":
    guarded("Australia businesses ABN (auto)", main)
