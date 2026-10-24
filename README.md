# data-jobs

Scheduled bulk imports of public company registers into a Postgres database (schema `prospecting`).
Each job downloads a public register file, parses it and sends records in batches to
`prospecting.bulk_ingest`. The database login used here can only call `bulk_run_start` and
`bulk_ingest`, and only for the sources listed in those functions.

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

## Insert-only loads (ABN)

The ABN job runs in insert-only mode (`run_job(..., insert_only=True)`): it calls
`prospecting.bulk_ingest_new_only`, which adds records whose key is new and leaves existing rows
untouched (`on conflict do nothing`). The register has about 4.7M rows and only a few thousand are
new each month; the old upsert rewrote every row each month. Side effect: changes to an existing
ABN (name, status, GST) are not picked up, and `last_seen_at` stays at the first load.
Other jobs still use `bulk_ingest` (upsert). The functions are in `sql/2026-10-24_abn_insert_only.sql`
and must be applied before this change is merged.

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
