"""Shared helpers: stream records into the prospecting raw layer.

Each job yields (key, posted_at, item) tuples. Records are sent in batches to
prospecting.bulk_ingest, which only accepts the bulk-job sources.
"""
import json
import os
import sys
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
# Shared with guarded() so an error is written to the run that failed, with the resume point.
STATE = {"run": None, "sent": 0, "skip": SKIP}


def valid_day(v):
    """Return v if it is a real YYYY-MM-DD date, else None."""
    if not v or len(v) != 10:
        return None
    try:
        date.fromisoformat(v)
        return v
    except ValueError:
        return None


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def _connect():
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL secret is missing")
    # prepare_threshold=None: the Supabase pooler in transaction mode cannot keep prepared statements
    return psycopg.connect(url, autocommit=True, prepare_threshold=None, connect_timeout=30)


def ingest(source, records, note=None, skip=None):
    """Load records. Returns (sent, finished). finished=False means time ran out;
    the caller should start a continuation with skip = skip + sent."""
    skip = SKIP if skip is None else skip
    conn = _connect()
    run = conn.execute("select prospecting.bulk_run_start(%s)", (source,)).fetchone()[0]
    STATE.update(run=run, sent=0, skip=skip)
    log(f"run {run} started for {source}")
    batch, sent, t0 = [], 0, time.time()

    def send(rows):
        """Send rows; on a statement timeout split them in half and try again."""
        nonlocal conn
        payload = json.dumps(rows, ensure_ascii=False)
        for attempt in range(5):
            try:
                if conn is None or conn.closed:
                    conn = _connect()
                conn.execute("select prospecting.bulk_ingest(%s, %s::jsonb, %s)", (run, payload, note))
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
        conn.execute("select prospecting.bulk_ingest(%s, '[]'::jsonb, %s)",
                     (run, f"paused: continue from skip={skip + sent}"))
    conn.close()
    return sent, finished


def run_job(source, records_fn, source_id=""):
    """Ingest, then start a continuation run if time ran out.
    records_fn() returns the record iterator. source_id identifies the downloaded file;
    if a continuation finds a different file than the chain started on, it loads the
    new file from the start (upserts make that safe) instead of skipping by a stale count."""
    skip = SKIP
    if SKIP and SOURCE_ID and source_id and SOURCE_ID != source_id:
        log(f"source changed since this chain started ({SOURCE_ID} -> {source_id}); loading the new file from the start")
        skip = 0
    sent, finished = ingest(source, records_fn(), skip=skip)
    if finished or LIMIT:
        return
    import requests
    repo = os.environ["GITHUB_REPOSITORY"]
    wf = os.environ["GITHUB_WORKFLOW_REF"].split("@")[0].split("/")[-1]
    ref = os.environ.get("GITHUB_REF_NAME", "main")
    r = requests.post(
        f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/dispatches",
        headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}", "Accept": "application/vnd.github+json"},
        json={"ref": ref, "inputs": {"skip": str(skip + sent), "record_limit": "0", "source_id": source_id or ""}},
        timeout=30,
    )
    r.raise_for_status()
    log(f"continuation started from record {skip + sent:,}")


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
        msg = "job error: " + "".join(traceback.format_exception_only(type(e), e)).strip()[:350]
        if STATE["run"]:
            msg += f" (resume with skip={STATE['skip'] + STATE['sent']})"
        log(msg)
        try:
            conn = _connect()
            run = STATE["run"] or conn.execute("select prospecting.bulk_run_start(%s)", (source,)).fetchone()[0]
            conn.execute("select prospecting.bulk_ingest(%s, '[]'::jsonb, %s)", (run, msg))
            conn.close()
        except Exception as e2:
            log(f"could not record error: {e2}")
        raise
