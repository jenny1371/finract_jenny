{#
  Change events -> the current row per primary key. Used by the staging models, which are incremental tables.

  - the same Kafka record delivered twice (at-least-once) counts once: de-duplicated on (topic, partition, offset)
  - the newest change per key wins, by source log position (lsn), then Kafka offset
  - a key whose newest change is a delete is returned with op = 'd'; the model deletes it from the table (see
    staging_post_hook)

  Incremental runs read only the files written since the previous run (ingested_at above the newest one already
  processed, minus a safety lookback), so the cost of a run follows the new data, not the whole history. The lookback
  re-reads a little, which is harmless: a re-read event never replaces a row that already holds a newer change (the
  not exists guard), so a run can be repeated and a late file cannot move a row backwards.

  Returns pk, op, after, changed_at_ms, lsn, "offset", ingested_at. Models must expose pk, _lsn and _offset.
#}
{% macro current_state(source_relation) %}
    with incoming as (
        select * from {{ source_relation }}
        {% if is_incremental() %}
        where ingested_at >= (select coalesce(max(_ingested_at), timestamp '1970-01-01') from {{ this }})
                             - interval '{{ var("incremental_lookback_minutes") }} minutes'
        {% endif %}
    ),
    deduplicated as (
        select distinct on (topic, partition, "offset") * from incoming
    ),
    ranked as (
        select
            pk, op, after, ts_ms as changed_at_ms, coalesce(lsn, 0) as lsn, "offset", ingested_at,
            row_number() over (partition by pk order by coalesce(lsn, 0) desc, "offset" desc) as recency
        from deduplicated
    )
    select pk, op, after, changed_at_ms, lsn, "offset", ingested_at
    from ranked
    where recency = 1
    {% if is_incremental() %}
      and not exists (select 1 from {{ this }} t
                      where t.pk = ranked.pk and (t._lsn, t._offset) >= (ranked.lsn, ranked."offset"))
    {% endif %}
{% endmacro %}

{# After the merge, rows whose newest change is a delete are removed. They must pass through the merge first so that
   the delete step replaces the old row; removing them afterwards keeps the tables free of deleted rows. #}
{% macro staging_post_hook() %}
    delete from {{ this }} where _is_deleted
{% endmacro %}
