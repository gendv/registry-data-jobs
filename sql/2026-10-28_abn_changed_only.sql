-- ABN job: update only changed records (spec e026 v1.3.1, task 3fcdff89). NOT APPLIED: needs the dry run and
-- Gen's approval (new functions and grants, changes to web_pending and feed_health, new column on ingest_runs).
-- Apply this BEFORE merging the job change, otherwise the ABN job fails on the missing functions.
--
-- Contents
--   1. ingest_runs.changed (additive column)
--   2. prospecting.abn_valid_rlud   helper: an 8 digit YYYYMMDD string that is a real date and not after p_max
--   3. prospecting.feed_ingest_changed  set-based insert of new ACT keys + update of changed rows (service_role only)
--   4. prospecting.bulk_ingest_changed  security definer wrapper, ABN source only (feed_writer only)
--   5. prospecting.bulk_last_success    security definer, one date from the newest valid complete marker (feed_writer only)
--   6. prospecting.web_pending          skips rows whose ABN.status is not ACT
--   7. prospecting.feed_health          'complete:%' and 'test:%' notes are not errors
-- bulk_ingest_new_only and bulk_ingest stay as they are (rollback path, other jobs).
-- Rollback: 2026-10-28_abn_changed_only_rollback.sql (restores 6 and 7). Functions and column may stay;
-- removing them goes through the manual queue.

-- 1
alter table prospecting.ingest_runs add column if not exists changed int not null default 0;

-- 2
create or replace function prospecting.abn_valid_rlud(p text, p_max date)
returns text
language sql
immutable
set search_path to ''
as $function$
  -- Nested CASE keeps the checks in order: a non-date never reaches make_date.
  select case when p is null or p !~ '^[0-9]{8}$' then null
    else case when substr(p, 1, 4)::int < 1 or substr(p, 5, 2)::int not between 1 and 12
                or substr(p, 7, 2)::int not between 1 and 31 then null
    else case when to_char(make_date(substr(p, 1, 4)::int, substr(p, 5, 2)::int, 1) + (substr(p, 7, 2)::int - 1), 'YYYYMMDD') <> p then null
    else case when p > to_char(p_max, 'YYYYMMDD') then null
    else p end end end end
$function$;

-- 3
create or replace function prospecting.feed_ingest_changed(
  p_run uuid, p_items jsonb, p_note text default null,
  p_same_before bigint default 0, p_max_same integer default 50000)
returns jsonb
language plpgsql
set search_path to ''
as $function$
declare
  v_src uuid; v_items jsonb; v_today date := current_date;
  v_n int := 0; v_new int := 0; v_max timestamptz;
  v_newer int := 0; v_same int := 0; v_same_cand int := 0;
  v_budget bigint; v_guard boolean := false; v_note text;
begin
  select source_id into v_src from prospecting.ingest_runs where id = p_run;
  if v_src is null then raise exception 'unknown run' using errcode = 'P0002'; end if;
  if jsonb_typeof(p_items) is distinct from 'array' then raise exception 'items must be a list' using errcode = '22023'; end if;
  -- Equal-date updates still allowed in this chain (hard maximum 200000, whatever the caller passes).
  v_budget := greatest(0, least(coalesce(p_max_same, 50000), 200000) - greatest(coalesce(p_same_before, 0), 0));
  select coalesce(jsonb_agg(e), '[]'::jsonb) into v_items from jsonb_array_elements(p_items) e where jsonb_typeof(e) = 'object';

  -- Step 1: new keys, ACT only, one sighting for posted_at. Existing keys are not touched (do nothing).
  -- The batch is deduplicated first (last occurrence wins).
  with b0 as (
    select t.key, nullif(t.posted_at, '')::timestamptz as posted, t.item, t.ordinality
    from rows from (jsonb_to_recordset(v_items) as (key text, posted_at text, item jsonb)) with ordinality as t(key, posted_at, item, ordinality)
    where nullif(t.key, '') is not null and jsonb_typeof(t.item) = 'object'
  ), b as (
    select distinct on (key) key, posted, item from b0 order by key, ordinality desc
  ), ins as (
    insert into prospecting.raw_records (source_id, source_row_key, data)
    select v_src, b.key, jsonb_build_object('item', b.item, 'first_seen_at', now(), 'last_seen_at', now())
    from b where b.item #>> '{ABN,status}' = 'ACT'
    on conflict (source_id, source_row_key) do nothing
    returning id, source_row_key
  ), sig as (
    insert into prospecting.raw_sightings (raw_id, posted_at)
    select ins.id, b.posted from ins join b on b.key = ins.source_row_key where b.posted is not null
    on conflict do nothing
    returning 1
  )
  select (select count(*) from b0), (select count(*) from ins),
         (select max(b.posted) from ins join b on b.key = ins.source_row_key)
  into v_n, v_new, v_max;

  -- Step 2: existing keys, only when the record changed. The new value is built inside this statement
  -- (data.web and every other top-level key stay as they are). No sighting, no watermark change.
  -- cat 'newer' = incoming rlud later than stored, or stored rlud missing or invalid;
  -- cat 'same'  = equal rlud and a different item. An invalid incoming rlud never updates.
  -- 'same' rows beyond the chain budget are not updated (guard).
  with b0 as (
    select t.key, t.item, t.ordinality
    from rows from (jsonb_to_recordset(v_items) as (key text, posted_at text, item jsonb)) with ordinality as t(key, posted_at, item, ordinality)
    where nullif(t.key, '') is not null and jsonb_typeof(t.item) = 'object'
  ), b as (
    select distinct on (key) key, item from b0 order by key, ordinality desc
  ), cand as (
    select r.id, b.item,
      case when vi.v is null then null
           when vs.v is null then 'newer'
           when vi.v > vs.v then 'newer'
           when vi.v = vs.v and r.data -> 'item' is distinct from b.item then 'same'
      end as cat
    from b
    join prospecting.raw_records r on r.source_id = v_src and r.source_row_key = b.key
    cross join lateral (select prospecting.abn_valid_rlud(b.item ->> 'recordLastUpdatedDate', v_today + 1) as v) vi
    cross join lateral (select prospecting.abn_valid_rlud(r.data #>> '{item,recordLastUpdatedDate}', v_today + 1) as v) vs
  ), ranked as (
    select id, item, cat, row_number() over (partition by cat order by id) as rn from cand where cat is not null
  ), upd as (
    update prospecting.raw_records r
    set data = jsonb_set(r.data, '{item}', k.item) || jsonb_build_object('last_seen_at', now(), 'changed_at', now())
    from ranked k
    where r.id = k.id and (k.cat = 'newer' or k.rn <= v_budget)
    returning k.cat
  )
  select count(*) filter (where cat = 'newer'), count(*) filter (where cat = 'same'),
         (select count(*) from ranked where cat = 'same')
  into v_newer, v_same, v_same_cand from upd;

  if v_same_cand > v_budget then
    v_guard := true;
    v_note := format('same_rlud_guard: %s equal-date changes in this batch, %s still allowed in this chain (max %s); equal-date rule stopped',
                     v_same_cand, v_budget, least(coalesce(p_max_same, 50000), 200000));
  end if;

  update prospecting.ingest_runs set pages = greatest(coalesce(pages, 0), 1), cards = coalesce(cards, 0) + v_n,
    new_jobs = coalesce(new_jobs, 0) + v_new, changed = changed + v_newer + v_same,
    watermark_after = greatest(watermark_after, watermark_before, v_max),
    finished_at = now(), note = coalesce(v_note, p_note, note)
  where id = p_run;

  update prospecting.sources s set row_count = coalesce(s.row_count, 0) + v_new,
    import_finished_at = now() where s.id = v_src;

  return jsonb_build_object('items', v_n, 'new_jobs', v_new, 'reposts', 0, 'changed', v_newer + v_same,
    'changed_newer', v_newer, 'changed_same', v_same, 'same_candidates', v_same_cand, 'guard', v_guard);
end
$function$;

-- 4
create or replace function prospecting.bulk_ingest_changed(
  p_run uuid, p_items jsonb, p_note text default null,
  p_same_before bigint default 0, p_max_same integer default 50000)
returns jsonb
language plpgsql
security definer
set search_path to ''
as $function$
declare v_src text;
begin
  select s.name into v_src from prospecting.ingest_runs r join prospecting.sources s on s.id = r.source_id where r.id = p_run;
  -- Only the ABN source may use changed-only ingest; the other bulk sources keep bulk_ingest.
  if v_src is null or v_src <> 'Australia businesses ABN (auto)' then
    raise exception 'run not allowed for changed-only bulk ingest';
  end if;
  return prospecting.feed_ingest_changed(p_run, p_items, p_note, p_same_before, p_max_same);
end $function$;

-- 5
create or replace function prospecting.bulk_last_success(p_source text)
returns date
language plpgsql
security definer
set search_path to ''
as $function$
declare v_d date;
begin
  if p_source is distinct from 'Australia businesses ABN (auto)' then
    raise exception 'source not allowed for last success: %', p_source;
  end if;
  -- Newest run whose note is a valid completion marker, minus 14 days. A marker date that is not a real date, or is
  -- later than the run's own start date + 1, is ignored and the next older marker is used (feed_writer can write any note).
  select to_date(m.d, 'YYYYMMDD') - 14 into v_d
  from prospecting.ingest_runs r
  join prospecting.sources s on s.id = r.source_id
  cross join lateral (select substring(r.note from '^complete: file=\S* max_rlud=(\d{8})$') as d) m
  where s.name = p_source and m.d is not null
    and prospecting.abn_valid_rlud(m.d, r.started_at::date + 1) is not null
  order by r.started_at desc
  limit 1;
  return v_d;
end $function$;

-- Owner: postgres, stated explicitly (the security definer functions run with the owner's rights).
alter function prospecting.abn_valid_rlud(text, date) owner to postgres;
alter function prospecting.feed_ingest_changed(uuid, jsonb, text, bigint, integer) owner to postgres;
alter function prospecting.bulk_ingest_changed(uuid, jsonb, text, bigint, integer) owner to postgres;
alter function prospecting.bulk_last_success(text) owner to postgres;

-- ACL: revoke before grant. Supabase default privileges give service_role execute on new functions,
-- so it is revoked explicitly on the two job functions (feed_writer is the only caller).
revoke all on function prospecting.abn_valid_rlud(text, date) from public, anon, authenticated;
revoke all on function prospecting.feed_ingest_changed(uuid, jsonb, text, bigint, integer) from public, anon, authenticated;
revoke all on function prospecting.bulk_ingest_changed(uuid, jsonb, text, bigint, integer) from public, anon, authenticated, service_role;
revoke all on function prospecting.bulk_last_success(text) from public, anon, authenticated, service_role;
grant execute on function prospecting.abn_valid_rlud(text, date) to service_role;
grant execute on function prospecting.feed_ingest_changed(uuid, jsonb, text, bigint, integer) to service_role;
grant execute on function prospecting.bulk_ingest_changed(uuid, jsonb, text, bigint, integer) to feed_writer;
grant execute on function prospecting.bulk_last_success(text) to feed_writer;

-- 6: skip cancelled rows in web enrichment (one added line)
CREATE OR REPLACE FUNCTION prospecting.web_pending(p_limit integer DEFAULT 20, p_daily_cap integer DEFAULT 400)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO ''
AS $function$
declare
  v_src uuid; v_used int; v_take int; v_out jsonb;
begin
  select id into v_src from prospecting.sources where name = 'Australia businesses ABN (auto)';
  select coalesce((select searches from prospecting.web_daily_usage where day = (now() at time zone 'Asia/Manila')::date), 0) into v_used;
  v_take := greatest(0, least(p_limit, 100, p_daily_cap - v_used));
  if v_take = 0 or v_src is null then return '[]'::jsonb; end if;

  with base as (
    select r.id, r.data -> 'item' i
    from prospecting.raw_records r
    where r.source_id = v_src
      and not (r.data ? 'web')
      and (r.data #>> '{item,GST,status}') = 'ACT'
      and (r.data #>> '{item,ABN,status}') = 'ACT'
      and (r.data #>> '{item,EntityType,EntityTypeInd}') in ('PRV', 'PUB', 'DTT', 'FPT', 'PTR', 'FUT', 'LPT', 'CUT', 'OIE', 'COP')
      and coalesce((r.data ->> 'web_attempts')::int, 0) < 3
      and coalesce((r.data ->> 'web_retry_after')::timestamptz, '-infinity'::timestamptz) <= now()
    order by r.id desc
    limit 2000
  ),
  named as (
    select b.id, b.i,
      array(select e #>> '{NonIndividualName,NonIndividualNameText}'
            from jsonb_array_elements(case jsonb_typeof(b.i -> 'OtherEntity') when 'array' then b.i -> 'OtherEntity'
                                           when 'object' then jsonb_build_array(b.i -> 'OtherEntity') else '[]'::jsonb end) e) other
    from base b
  ),
  pick as (
    select n.* from named n
    where exists (select 1 from unnest(array[n.i #>> '{MainEntity,NonIndividualName,NonIndividualNameText}'] || n.other) x
                  where x !~* '(trust|nominee|investment|holding|superannuation|super fund|^a\.?c\.?n\.? )')
    order by n.id desc
    limit v_take
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'raw_id', id,
    'abn', i #>> '{ABN,text}',
    'acn', i #>> '{ASICNumber,text}',
    'name', i #>> '{MainEntity,NonIndividualName,NonIndividualNameText}',
    'other', array_to_string(other, ' | '),
    'state', i #>> '{MainEntity,BusinessAddress,AddressDetails,State}',
    'postcode', i #>> '{MainEntity,BusinessAddress,AddressDetails,Postcode}')), '[]'::jsonb)
  into v_out from pick;
  return v_out;
end $function$;

-- 7: complete: and test: notes are not errors (added to both note filters)
CREATE OR REPLACE FUNCTION prospecting.feed_health()
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO ''
AS $function$
declare
  v jsonb := '[]'::jsonb;
  s record;
  v_limit int;
  v_last_run timestamptz; v_last_new timestamptz;
  v_n int; v_pending int; v_last_extract timestamptz;
  v_stale interval;
  v_failed int := 0;
begin
  for s in select id, name, extract_enabled from prospecting.sources
           where name like '% (auto)' and name <> 'Upwork job posts (auto)' order by name loop
    v_limit := case
      when s.name like 'OnlineJobs%' then 6
      when s.name like 'Himalayas%' then 6
      when s.name like 'Jobicy%' then 24
      when s.name like 'Norway%' then 96
      when s.name like 'Florida%' then 120
      when s.name like 'Australia businesses%' then 900
      when s.name like 'Australia%' then 200
      else 72 end;
    v_stale := case when s.name like 'Australia businesses%' then interval '35 days' when s.name like 'UK companies%' then interval '26 hours' when s.name like 'Florida%' then interval '100 hours' when s.name like 'Norway%' or s.name like 'Reed%' or s.name like 'Australia%' or s.name like 'Ireland%' then interval '26 hours' else interval '4 hours' end;
    select max(started_at), max(started_at) filter (where new_jobs > 0)
      into v_last_run, v_last_new
    from prospecting.ingest_runs where source_id = s.id;

    if v_last_run is null then continue; end if;

    if v_last_run < now() - v_stale then
      v := v || to_jsonb(format('%s：已經 %s 小時沒有執行（應該%s一次）', s.name,
                                 floor(extract(epoch from now() - v_last_run) / 3600),
                                 case when s.name like 'Australia businesses%' then '每月' when s.name like 'Norway%' or s.name like 'Reed%' then '每天' else '每 3 小時' end));
    elsif v_last_new is null or v_last_new < now() - make_interval(hours => v_limit) then
      v := v || to_jsonb(format('%s：超過 %s 小時沒有任何新資料，網站可能改版或擋住了', s.name, v_limit));
    end if;

    select count(*) into v_n from prospecting.ingest_runs
    where source_id = s.id and started_at > now() - interval '24 hours' and gap_suspected;
    if v_n > 0 then
      v := v || to_jsonb(format('%s：過去 24 小時有 %s 次可能漏抓（翻到上限還沒接上上一輪）', s.name, v_n));
    end if;

    select count(*) into v_n from prospecting.ingest_runs
    where source_id = s.id and started_at > now() - interval '24 hours'
      and note is not null and note not like 'manual backfill%' and note not like 'partial:%' and note not like 'paused:%' and note not like 'complete:%' and note not like 'test:%';
    if v_n > 0 then
      v := v || to_jsonb(format('%s：過去 24 小時有 %s 次執行留下錯誤訊息：%s', s.name, v_n,
        (select left(note, 120) from prospecting.ingest_runs
         where source_id = s.id and started_at > now() - interval '24 hours' and note is not null and note not like 'manual backfill%' and note not like 'partial:%' and note not like 'paused:%' and note not like 'complete:%' and note not like 'test:%'
         order by started_at desc limit 1)));
    end if;

    select count(*) into v_n from prospecting.ingest_runs
    where source_id = s.id and started_at > now() - interval '24 hours' and note like 'partial:%';
    if v_n > 0 then
      v := v || to_jsonb(format('%s：過去 24 小時有 %s 次只抓到部分資料（上限截斷或有讀不了的列）：%s', s.name, v_n,
        (select left(note, 120) from prospecting.ingest_runs
         where source_id = s.id and started_at > now() - interval '24 hours' and note like 'partial:%'
         order by started_at desc limit 1)));
    end if;

    if exists (select 1 from prospecting.ingest_runs r1
               where r1.source_id = s.id and r1.note like 'paused:%' and r1.finished_at < now() - interval '2 hours'
                 and r1.started_at = (select max(r2.started_at) from prospecting.ingest_runs r2 where r2.source_id = s.id)) then
      v := v || to_jsonb(format('%s：大檔匯入暫停後，接續的執行沒有開始（到 GitHub Actions 手動接續）', s.name));
    end if;

    select count(*) into v_n from prospecting.ingest_runs
    where source_id = s.id and finished_at is null
      and started_at between now() - interval '24 hours' and now() - interval '30 minutes';
    if v_n > 0 then
      v := v || to_jsonb(format('%s：有 %s 次執行開始了但沒有結束（卡住）', s.name, v_n));
    end if;

    if s.extract_enabled then
      select count(*) filter (where not r.data ? 'extract' and r.data ->> 'removed_at' is null
                                and r.data ->> 'extract_skipped_at' is null
                                and coalesce((r.data ->> 'extract_attempts')::int, 0) < 3
                                and (s.name not like 'OnlineJobs%' or r.data ? 'detail')),
             max((r.data #>> '{extract,extracted_at}')::timestamptz),
             count(*) filter (where not r.data ? 'extract' and r.data ->> 'extract_skipped_at' is null
                                and coalesce((r.data ->> 'extract_attempts')::int, 0) >= 3
                                and (r.data ->> 'extract_last_attempt_at')::timestamptz > now() - interval '24 hours')
        into v_pending, v_last_extract, v_n
      from prospecting.raw_records r where r.source_id = s.id;
      v_failed := v_failed + coalesce(v_n, 0);
      if v_pending > 0 and (v_last_extract is null or v_last_extract < now() - interval '6 hours') then
        v := v || to_jsonb(format('%s：模型抽取超過 6 小時沒有進度，還有 %s 筆等待', s.name, v_pending));
      end if;
    end if;
  end loop;

  -- Counts extract-enabled sources named '% (auto)' (all of them today).
  if v_failed >= 10 then
    v := v || to_jsonb(format('模型抽取：過去 24 小時有 %s 筆失敗滿 3 次，請到後台 Prospecting > Overview 看原因', v_failed));
  end if;

  return v;
end
$function$;
