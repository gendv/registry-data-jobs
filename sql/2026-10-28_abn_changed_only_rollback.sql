-- Rollback for 2026-10-28_abn_changed_only.sql. NOT APPLIED.
-- Restores the definitions of prospecting.web_pending and prospecting.feed_health exactly as they were
-- on 2026-10-28 (taken from pg_get_functiondef on production, md5 checked). Run this only together with
-- reverting the job (merge revert, see README: no ABN run may be active).
--
-- Not restored here, on purpose:
--   * The web_pending ABN.status filter stays unless cancelled rows should go back into web enrichment;
--     restoring the old web_pending below does put them back, so decide before running.
--   * The new functions (prospecting.abn_valid_rlud, feed_ingest_changed, bulk_ingest_changed,
--     bulk_last_success) and the column prospecting.ingest_runs.changed may stay: they are unused once the
--     job is reverted and harmless. Removing them goes through the manual queue (ops.gen-manual-queue).
--   * Rows already updated by the changed-only job keep their new item (it was replaced whole).

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
      and note is not null and note not like 'manual backfill%' and note not like 'partial:%' and note not like 'paused:%';
    if v_n > 0 then
      v := v || to_jsonb(format('%s：過去 24 小時有 %s 次執行留下錯誤訊息：%s', s.name, v_n,
        (select left(note, 120) from prospecting.ingest_runs
         where source_id = s.id and started_at > now() - interval '24 hours' and note is not null and note not like 'manual backfill%' and note not like 'partial:%' and note not like 'paused:%'
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
