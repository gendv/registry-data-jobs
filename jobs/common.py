"""Shared helpers: stream records into the prospecting raw layer.

Each job yields (key, posted_at, item) tuples. Records are sent in batches to
prospecting.bulk_ingest, which only accepts the bulk-job sources.
"""
import json
import os
import sys
import statistics
import threading
import time
from datetime import date

import psycopg

BATCH = int(os.environ.get("BATCH_SIZE", "2000"))
LIMIT = int(os.environ.get("RECORD_LIMIT", "0"))  # 0 = no limit (use a small number for a test run)
SKIP = int(os.environ.get("SKIP", "0") or 0)  # records already loaded by an earlier part of this run
# Pacing: keeps growth slow enough for Supabase disk auto-scaling (adds 50%, at most every 4 hours).
MAX_PER_HOUR = int(os.environ.get("MAX_PER_HOUR", "400000"))
# The workflow kills the job at 350 minutes; hand over to a fresh run well before that.
# Counted from process start, so download time is included.
MAX_MINUTES = int(os.environ.get("MAX_MINUTES", "290"))
T0 = time.time()
# Identity of the source file this chain started on (set automatically on continuations).
SOURCE_ID = os.environ.get("SOURCE_ID", "")
# Server-side cap per bulk_ingest call. A call that exceeds it raises QueryCanceled, which send() answers by splitting the batch.
STATEMENT_TIMEOUT_S = int(os.environ.get("STATEMENT_TIMEOUT_S", "120"))
# If the main loop has not handed over this long after MAX_MINUTES (stuck in a call or the iterator), the watchdog does it.
WATCHDOG_GRACE_MIN = int(os.environ.get("WATCHDOG_GRACE_MIN", "15"))
# Shared with guarded() so an error is written to the run that failed, with the resume point.
# Same-rlud ceiling for the ABN "changed" mode (spec 3.1): updates of records whose recordLastUpdatedDate
# is equal to the stored one, counted across the whole chain. The function enforces its own hard maximum.
MAX_CHANGED_SAME = int(os.environ.get("MAX_CHANGED_SAME", "50000"))
# "chain" carries what a continuation needs (cutoff, max_nonact, nonact_count, chain_date); empty for other jobs.
STATE = {"run": None, "sent": 0, "skip": SKIP, "source_id": "", "handed_over": False, "insert_only": False,
         "changed": False, "chain": {}, "changed_same": 0, "changed_newer": 0, "changed_total": 0,
         "new_total": 0, "durations": []}


class GuardError(Exception):
    """A guard stopped the job. `note` is written to the run as is, so feed_health shows it."""

    def __init__(self, note):
        super().__init__(note)
        self.note = note


def valid_day(v):
    """Return v if it is a real YYYY-MM-DD date, else None."""
    if not v or len(v) != 10:
        return None
    try:
        day = date.fromisoformat(v)
    except ValueError:
        return None
    # Source files contain typos such as 2124-09-06; a date more than a year ahead is treated as missing.
    if (day - date.today()).days > 366:
        return None
    return v


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def _connect():
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL secret is missing")
    # prepare_threshold=None: the Supabase pooler in transaction mode cannot keep prepared statements.
    # The TCP settings make a silently dead connection raise OperationalError (and so reach the retry
    # logic) instead of blocking forever; the server-side statement timeout cannot help when the
    # network path itself has stalled.
    return psycopg.connect(url, autocommit=True, prepare_threshold=None, connect_timeout=30,
                           keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
                           tcp_user_timeout=(STATEMENT_TIMEOUT_S + 60) * 1000)


def _ingest_sql():
    """The database call for this run. Modes:
    changed     prospecting.bulk_ingest_changed: inserts new keys, updates an existing row only when the
                record changed (recordLastUpdatedDate newer, or same date with a different item);
    insert_only prospecting.bulk_ingest_new_only: on conflict do nothing;
    default     prospecting.bulk_ingest: upsert of every row."""
    if STATE["changed"]:
        return "select prospecting.bulk_ingest_changed(%s, %s::jsonb, %s, %s, %s)"
    fn = "bulk_ingest_new_only" if STATE["insert_only"] else "bulk_ingest"
    return f"select prospecting.{fn}(%s, %s::jsonb, %s)"


def _resume_hint():
    """' cutoff=YYYY-MM-DD' for jobs that carry a cutoff, so every resume note says how to continue."""
    c = STATE["chain"].get("cutoff")
    return f" cutoff={c}" if c else ""


def _ingest_call(conn, run, payload, note):
    """One bulk_ingest call with a per-call statement timeout. The timeout is set with
    set_config(..., is_local) inside a transaction rather than as a startup option, because the
    Supabase pooler in transaction mode does not pass startup options through."""
    t = time.time()
    with conn.transaction():
        conn.execute("select set_config('statement_timeout', %s, true)", (f"{STATEMENT_TIMEOUT_S}s",))
        if STATE["changed"]:
            params = (run, payload, note, STATE["changed_same"], MAX_CHANGED_SAME)
        else:
            params = (run, payload, note)
        cur = conn.execute(_ingest_sql(), params)
        res = cur.fetchone()[0] if cur is not None else None
    if STATE["changed"]:
        took = time.time() - t
        STATE["durations"].append(took)
        log(f"call {took:.2f}s")
        _count_result(res)
    return res


def _count_result(res):
    """Add one call's counters to the chain totals; stop the job if the same-rlud guard tripped."""
    if not isinstance(res, dict):
        return
    STATE["changed_same"] += int(res.get("changed_same") or 0)
    STATE["changed_newer"] += int(res.get("changed_newer") or 0)
    STATE["changed_total"] += int(res.get("changed") or 0)
    STATE["new_total"] += int(res.get("new_jobs") or 0)
    if res.get("guard"):
        raise GuardError(f"same_rlud_guard: equal-date updates passed {MAX_CHANGED_SAME:,} in this chain "
                         f"(now {STATE['changed_same']:,}); the equal-date rule was switched off for the rest of the batch. "
                         "Check the parser or the register schema before raising MAX_CHANGED_SAME")


def _report_durations():
    d = STATE["durations"]
    if d:
        log(f"call time p50 {statistics.median(d):.2f}s, max {max(d):.2f}s over {len(d):,} calls")


def _dispatch_continuation(skip, source_id):
    """Start a new run of this workflow that resumes at record `skip`. A job with a chain (ABN changed mode)
    also passes on its cutoff, limits and counters; this is the one place both the normal hand-over and the
    watchdog use."""
    import requests
    inputs = {"skip": str(skip), "record_limit": "0", "source_id": source_id or ""}
    chain = STATE["chain"]
    if chain:
        inputs.update({k: str(v) for k, v in chain.items()})
        inputs["changed_same"] = str(STATE["changed_same"])
    repo = os.environ["GITHUB_REPOSITORY"]
    wf = os.environ["GITHUB_WORKFLOW_REF"].split("@")[0].split("/")[-1]
    ref = os.environ.get("GITHUB_REF_NAME", "main")
    r = requests.post(
        f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/dispatches",
        headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}", "Accept": "application/vnd.github+json"},
        json={"ref": ref, "inputs": inputs},
        timeout=30,
    )
    r.raise_for_status()
    log(f"continuation started from record {skip:,}")


def _watchdog():
    """Backstop for a main thread that is stuck (a database call or the record iterator that never
    returns). The normal MAX_MINUTES check only runs while the loop is moving; this one runs on a
    timer. WATCHDOG_GRACE_MIN after the deadline it records the resume point, starts the
    continuation from what is known to be loaded, and exits non-zero so the stall shows up as a failed run."""
    deadline = T0 + (MAX_MINUTES + WATCHDOG_GRACE_MIN) * 60
    while time.time() < deadline:
        if STATE["handed_over"]:
            return
        time.sleep(15)
    if STATE["handed_over"]:
        return
    resume = STATE["skip"] + STATE["sent"]
    log(f"watchdog: no hand-over {WATCHDOG_GRACE_MIN} min after MAX_MINUTES={MAX_MINUTES}; main thread is stuck")
    try:
        if STATE["run"]:
            conn = _connect()
            _ingest_call(conn, STATE["run"], "[]", f"stalled: continue from skip={resume}{_resume_hint()}")
            conn.close()
    except Exception as e:
        log(f"watchdog: could not record the stall: {e}")
    try:
        if not LIMIT:
            _dispatch_continuation(resume, STATE["source_id"])
    except Exception as e:
        log(f"watchdog: could not start the continuation (resume with skip={resume}): {e}")
    os._exit(3)


def ingest(source, records, note=None, skip=None, insert_only=None):
    """Load records. Returns (sent, finished). finished=False means time ran out;
    the caller should start a continuation with skip = skip + sent.
    insert_only=True only adds records whose key is new and leaves existing rows untouched."""
    skip = SKIP if skip is None else skip
    if insert_only is not None:
        STATE["insert_only"] = insert_only
    conn = _connect()
    run = conn.execute("select prospecting.bulk_run_start(%s)", (source,)).fetchone()[0]
    STATE.update(run=run, sent=0, skip=skip)
    mode = "changed-only" if STATE["changed"] else "insert-only" if STATE["insert_only"] else "upsert"
    log(f"run {run} started for {source} ({mode})")
    threading.Thread(target=_watchdog, daemon=True).start()
    batch, sent, t0 = [], 0, time.time()

    def send(rows):
        """Send rows; on a statement timeout split them in half and try again."""
        nonlocal conn
        payload = json.dumps(rows, ensure_ascii=False)
        for attempt in range(5):
            try:
                if conn is None or conn.closed:
                    conn = _connect()
                _ingest_call(conn, run, payload, note)
                return
            except psycopg.errors.QueryCanceled:
                if len(rows) > 1:
                    log(f"timeout on {len(rows)} rows, splitting")
                    half = len(rows) // 2
                    send(rows[:half])
                    send(rows[half:])
                    return
                raise
            except psycopg.OperationalError as e:
                log(f"retry {attempt + 1} after error: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
                time.sleep(10 * (attempt + 1))
        raise RuntimeError("bulk_ingest failed 5 times")

    def flush():
        nonlocal batch, sent
        if not batch:
            return
        send(batch)
        sent += len(batch)
        STATE["sent"] = sent
        batch = []
        if sent % (BATCH * 50) == 0:
            log(f"{sent:,} records sent ({sent / max(time.time() - t0, 1):.0f}/s)")

    skipped = 0
    finished = True
    paced_from = None
    for key, posted_at, item in records:
        if skipped < skip:
            skipped += 1
            continue
        if time.time() - T0 > MAX_MINUTES * 60:  # also checked per record, not only after a flush
            finished = False
            break
        if paced_from is None:
            paced_from = time.time()
        if key is None or str(key).strip() == "":
            continue
        batch.append({"key": str(key).strip(), "posted_at": valid_day(posted_at), "item": item})
        if len(batch) >= BATCH:
            flush()
            ahead = sent / MAX_PER_HOUR * 3600 - (time.time() - paced_from)
            if ahead > 0:
                time.sleep(ahead)
            if time.time() - T0 > MAX_MINUTES * 60:
                finished = False
                break
        if LIMIT and sent + len(batch) >= LIMIT:
            break
    flush()
    log(f"{'done' if finished else 'paused'}: {sent:,} records for {source} in {time.time() - t0:.0f}s (skipped {skipped:,})")
    if not finished:
        _ingest_call(conn, run, "[]", f"paused: continue from skip={skip + sent}{_resume_hint()}")
    conn.close()
    STATE["handed_over"] = True
    if STATE["changed"]:
        log(f"changed {STATE['changed_total']:,} (newer {STATE['changed_newer']:,}, same date {STATE['changed_same']:,} "
            f"in this chain), new {STATE['new_total']:,}")
        _report_durations()
    return sent, finished


def effective_skip(source_id):
    """Where this run starts. A continuation that finds a different file than its chain started on
    starts from 0 (the caller keeps the chain's cutoff)."""
    if SKIP and SOURCE_ID and source_id and SOURCE_ID != source_id:
        return 0
    return SKIP


def _write_note(note):
    """Put a note on the current run without sending records (the original function, which always exists)."""
    conn = _connect()
    try:
        conn.execute("select prospecting.bulk_ingest(%s, '[]'::jsonb, %s)", (STATE["run"], note))
    finally:
        conn.close()


def run_job(source, records_fn, source_id="", insert_only=False, changed=False, on_complete=None):
    """Ingest, then start a continuation run if time ran out.
    records_fn() returns the record iterator. source_id identifies the downloaded file;
    if a continuation finds a different file than the chain started on, it loads the
    new file from the start (upserts make that safe) instead of skipping by a stale count.
    changed=True uses the changed-only database call. When the whole file has been read (no record limit),
    on_complete() may return a completion note, which is written to the run; a record-limited run
    in changed mode writes 'test: record_limit=N' instead."""
    skip = effective_skip(source_id)
    if skip != SKIP:
        log(f"source changed since this chain started ({SOURCE_ID} -> {source_id}); loading the new file from the start")
    STATE["source_id"] = source_id or ""
    STATE["insert_only"] = insert_only
    STATE["changed"] = changed
    sent, finished = ingest(source, records_fn(), skip=skip)
    if finished:
        note = None
        if changed and LIMIT:
            note = f"test: record_limit={LIMIT}"
        elif changed and on_complete:
            note = on_complete()
        if note:
            _write_note(note)
            log(note)
    if finished or LIMIT:
        return
    _dispatch_continuation(skip + sent, source_id)


def clean(d):
    """Drop empty values, recursively."""
    if isinstance(d, dict):
        out = {}
        for k, v in d.items():
            v = clean(v)
            if v in (None, "", [], {}):
                continue
            out[k] = v
        return out
    if isinstance(d, list):
        return [x for x in (clean(v) for v in d) if x not in (None, "", [], {})]
    if isinstance(d, str):
        return d.strip()
    return d


def guarded(source, fn):
    """Run fn(); on any error, record it on the run that failed (or a new one if none
    started yet), with the point to resume from, so it shows up in feed health."""
    import traceback
    try:
        fn()
    except BaseException as e:
        if isinstance(e, KeyboardInterrupt):
            raise
        if isinstance(e, GuardError):
            msg = e.note[:350]
        else:
            msg = "job error: " + "".join(traceback.format_exception_only(type(e), e)).strip()[:350]
        if STATE["run"]:
            msg += f" (resume with skip={STATE['skip'] + STATE['sent']}{_resume_hint()})"
        log(msg)
        try:
            conn = _connect()
            run = STATE["run"] or conn.execute("select prospecting.bulk_run_start(%s)", (source,)).fetchone()[0]
            # Always the original function: it exists even if the insert-only functions were never applied,
            # and with an empty list both only update the run's note.
            conn.execute("select prospecting.bulk_ingest(%s, '[]'::jsonb, %s)", (run, msg))
            conn.close()
        except Exception as e2:
            log(f"could not record error: {e2}")
        raise
