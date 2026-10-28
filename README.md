# data-jobs

Scheduled bulk imports of public company registers into a Postgres database (schema `prospecting`).
Each job downloads a public register file, parses it and sends records in batches to
`prospecting.bulk_ingest` (the ABN job uses `bulk_ingest_changed`). The database login used here can only call
`bulk_run_start`, `bulk_ingest`, `bulk_ingest_new_only`, `bulk_ingest_changed` and `bulk_last_success`, and only for the sources listed in those functions.

| Workflow | Source | Schedule (UTC) |
|---|---|---|
| Florida companies (quarterly file) | Florida Sunbiz SFTP, cordata zip | 20th of Jan, Apr, Jul, Oct |
| Australia ABN register (monthly) | data.gov.au ABN bulk extract (active, non-individual) | 8th monthly |
| UK companies (monthly snapshot) | Companies House free company data product | 12th monthly |
| Norway sub-units (monthly) | Brreg underenheter bulk download | 3rd monthly |

Only one job runs at a time (shared concurrency group).

Secrets: `DATABASE_URL`, `FL_SFTP_USER`, `FL_SFTP_PASSWORD`.

Test run: Actions tab, pick a workflow, Run workflow, set record_limit to 2000.

Attribution: Contains Australian, UK and Norwegian public sector data under their open licences (CC BY 3.0 AU, OGL, NLOD).

## ABN: changed-only loads

The ABN job calls `prospecting.bulk_ingest_changed` (mode `changed` in `common.py`). The register has about
4.7M rows and only a few thousand are new or changed each month; the old upsert rewrote every row each month
and left about 4 GB of dead rows, and the insert-only version (`bulk_ingest_new_only`, still available as the
rollback path) never saw changes. Now:

- Every active (ACT) non-individual record is sent. The database inserts new keys, and updates an existing
  row only when the record changed: its `recordLastUpdatedDate` (rlud) is newer, the stored rlud is missing or
  invalid, or the date is equal but the item differs. Unchanged rows are not touched (no new row version).
  An invalid incoming rlud (not 8 digits, not a real date, or later than tomorrow) never updates a row.
- Updates replace `data.item` only, keep every other key (such as `data.web`), and set `last_seen_at` and
  `data.changed_at`. They write no sighting and do not move the watermark. `last_seen_at` therefore changes only
  when a row changes.
- Cancelled (non-ACT) non-individual records are sent too when `greatest(rlud, ABNStatusFromDate)` is on or after
  the cutoff, or has no usable date. They only update rows that already exist; they are never inserted. The
  web enrichment (`prospecting.web_pending`) skips rows whose ABN status is not ACT. Nothing is deleted.
- Cutoff: workflow input `cutoff` (`YYYY-MM-DD`, or `all`), else `prospecting.bulk_last_success` (max rlud of the
  newest complete run minus 14 days), else the fixed floor 2026-09-14. It is read once at the start of a chain and
  passed on to every continuation; a continuation without a cutoff fails.
- Stream order: ACT records in file order, then the cancelled records that pass the cutoff, in file order (spooled
  to a queue file on the runner). Every run re-parses the whole file, so skip counts line up.
- Volume guard: the first run of a chain parses the whole file and counts the cancelled records before sending
  anything. Over `max_nonact` (default 500,000) it stops with note `nonact_guard: ...` (shown by feed_health);
  rerun with a higher `max_nonact` or another cutoff. Continuations recount and fail if the count differs.
- Same-date ceiling: updates where the rlud is equal but the item differs are counted across the chain; past
  `MAX_CHANGED_SAME` (50,000) the database stops applying that rule and the job fails with `same_rlud_guard: ...`
  (a sign that the parser or the register schema changed).
- Run notes: a run that read the whole file writes `complete: file=<id> max_rlud=<YYYYMMDD>`; a run with
  `record_limit` writes `test: record_limit=N`. Neither is an error in feed_health.
- `count_only=1` parses the file and logs the counts (ACT, cancelled at or after the cutoff, cancelled total,
  individuals, S2) with no database connection and nothing sent. Use it to size the cancelled volume.
- Each database call's duration is logged; the run ends with p50 and max.

Merge and rollback: merge only when no ABN run is active (`gh run list --workflow abn-bulk.yml` shows nothing
in progress, queued, waiting or pending, and the latest ABN run finished more than 30 minutes ago), and apply
`sql/2026-10-28_abn_changed_only.sql` first. To roll back, revert the merge under the same rule; the
web_pending filter stays. The new functions and the `ingest_runs.changed` column can stay (removal goes through
the manual queue); `sql/2026-10-28_abn_changed_only_rollback.sql` restores the old `web_pending` and `feed_health`.
Other jobs still use `bulk_ingest` (upsert). The insert-only functions are in `sql/2026-10-24_abn_insert_only.sql`.

## Pacing and long runs

Jobs load at most 400,000 records per hour (`MAX_PER_HOUR`) so database disk auto-scaling can keep up.
The workflow stops a job at 350 minutes, so after 290 minutes (counted from start, download included)
a job stops cleanly, records where it paused, and starts a new run of itself with `skip` set to the
number of records already loaded and `source_id` set to the file it was reading. If the source file
has changed by then, the new run loads the new file from the start. Re-loading a record is harmless
(upsert by key).

If a database call or the record iterator hangs, a watchdog thread starts the continuation itself
`WATCHDOG_GRACE_MIN` (15) minutes after `MAX_MINUTES` and exits non-zero so the stall is visible.
Each `bulk_ingest` call also has a 120 s statement timeout (`STATEMENT_TIMEOUT_S`) and TCP keepalives,
so a stuck call raises and goes through the existing split/retry logic.

The Florida workflow caches the downloaded zip (key: filename, mtime, size), so a parse failure
does not force another download. Members that Python's zipfile cannot decompress (Deflate64) are
streamed through `unzip -p`.
