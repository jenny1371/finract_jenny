{#
  Change events -> the current row per primary key.
  - the same Kafka record delivered twice (at-least-once) counts once: de-duplicated on (topic, partition, offset)
  - the newest change per key wins, by source log position (lsn), then Kafka offset
  - a key whose newest change is a delete disappears
  Returns the columns pk and after (the row as JSON) plus the position of that change.
#}
{% macro current_state(source_relation) %}
    with deduplicated as (
        select distinct on (topic, partition, "offset") *
        from {{ source_relation }}
    ),
    ranked as (
        select
            pk, op, after, ts_ms, lsn, "offset",
            row_number() over (partition by pk order by lsn desc nulls last, "offset" desc) as recency
        from deduplicated
    )
    select pk, after, ts_ms as changed_at_ms, lsn
    from ranked
    where recency = 1 and op <> 'd'
{% endmacro %}
