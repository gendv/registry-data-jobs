-- Insert-only ingest for the ABN bulk job. NOT APPLIED: needs a dry run and Gen's approval
-- (new functions and grants). Apply this BEFORE merging the job change, otherwise the ABN job
-- fails on the missing function.
--
-- Why new functions and not a new parameter on feed_ingest / bulk_ingest: adding a defaulted
-- parameter creates an overload, and calls with the old argument list then fail with
-- "function is not unique". The existing functions stay exactly as they are, so every other
-- source and caller is unaffected.
-- Rollback: drop function prospecting.bulk_ingest_new_only(uuid, jsonb, text);
--           drop function prospecting.feed_ingest_new_only(uuid, jsonb, integer, boolean, text);

create or replace function prospecting.feed_ingest_new_only(
  p_run uuid, p_items jsonb, p_pages integer, p_gap boolean, p_note text default null)
returns jsonb
language plpgsql
set search_path to ''
as $function$
declare
  v_src uuid; c jsonb; v_id bigint; v_posted timestamptz;
  v_new_n int := 0; v_n int := 0; v_max timestamptz;
begin
  select source_id into v_src from prospecting.ingest_runs where id = p_run;
  if v_src is null then raise exception 'unknown run' using errcode = 'P0002'; end if;
  if jsonb_typeof(p_items) is distinct from 'array' then raise exception 'items must be a list' using errcode = '22023'; end if;
  for c in select * from jsonb_array_elements(p_items) loop
    continue when nullif(c ->> 'key', '') is null;
    continue when jsonb_typeof(c -> 'item') is distinct from 'object';
    v_n := v_n + 1;
    v_posted := nullif(c ->> 'posted_at', '')::timestamptz;
    v_max := greatest(v_max, v_posted);
    v_id := null;
    -- Existing keys are skipped: no update, no new row version, no sighting.
    insert into prospecting.raw_records (source_id, source_row_key, data)
    values (v_src, c ->> 'key', jsonb_build_object('item', c -> 'item', 'first_seen_at', now(), 'last_seen_at', now()))
    on conflict (source_id, source_row_key) do nothing
    returning id into v_id;
    if v_id is not null then
      v_new_n := v_new_n + 1;
      if v_posted is not null then
        insert into prospecting.raw_sightings (raw_id, posted_at) values (v_id, v_posted) on conflict do nothing;
      end if;
    end if;
  end loop;

  update prospecting.ingest_runs set pages = greatest(coalesce(pages, 0), p_pages), cards = coalesce(cards, 0) + v_n,
    new_jobs = coalesce(new_jobs, 0) + v_new_n,
    gap_suspected = coalesce(gap_suspected, false) or coalesce(p_gap, false),
    watermark_after = greatest(watermark_after, watermark_before, v_max),
    finished_at = now(), note = coalesce(p_note, note)
  where id = p_run;

  update prospecting.sources s set row_count = coalesce(s.row_count, 0) + v_new_n,
    import_finished_at = now() where s.id = v_src;

  return jsonb_build_object('items', v_n, 'new_jobs', v_new_n, 'reposts', 0);
end
$function$;

create or replace function prospecting.bulk_ingest_new_only(p_run uuid, p_items jsonb, p_note text default null)
returns jsonb
language plpgsql
security definer
set search_path to ''
as $function$
declare v_src text;
begin
  select s.name into v_src from prospecting.ingest_runs r join prospecting.sources s on s.id = r.source_id where r.id = p_run;
  -- Only the ABN source may use insert-only; the other bulk sources keep bulk_ingest.
  if v_src is null or v_src <> 'Australia businesses ABN (auto)' then
    raise exception 'run not allowed for insert-only bulk ingest';
  end if;
  return to_jsonb(prospecting.feed_ingest_new_only(p_run, p_items, 1, false, p_note));
end $function$;

revoke all on function prospecting.feed_ingest_new_only(uuid, jsonb, integer, boolean, text) from public, anon, authenticated;
revoke all on function prospecting.bulk_ingest_new_only(uuid, jsonb, text) from public, anon, authenticated;
grant execute on function prospecting.feed_ingest_new_only(uuid, jsonb, integer, boolean, text) to service_role;
grant execute on function prospecting.bulk_ingest_new_only(uuid, jsonb, text) to feed_writer;
