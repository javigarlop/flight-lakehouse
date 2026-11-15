-- One row per dbt run: how far behind real time the data in gold is.
-- event_to_gold_s is the age of the newest position silver held when this run read it;
-- p50 and p95 ingest latency cover event_time to bronze ingestion over the last hour.
-- Replayed recordings have old event times, so their lag is large by design.

{{ config(incremental_strategy="append") }}

{% set now = "cast(current_timestamp as timestamp(6) with time zone)" %}

with positions as (
    select
        event_time,
        ingested_at
    from {{ source("silver", "flight_positions") }}
),

latest as (
    select
        max(event_time) as latest_event_time,
        max(ingested_at) as latest_ingested_at
    from positions
),

last_hour as (
    select
        approx_percentile(date_diff('millisecond', event_time, ingested_at), 0.5) / 1e3
            as p50_ingest_latency_s,
        approx_percentile(date_diff('millisecond', event_time, ingested_at), 0.95) / 1e3
            as p95_ingest_latency_s,
        count(*) as positions_last_hour
    from positions
    where event_time >= {{ now }} - interval '1' hour
)

select
    -- Iceberg stores microseconds; current_timestamp has millisecond precision.
    {{ now }} as measured_at,
    latest.latest_event_time,
    latest.latest_ingested_at,
    date_diff('second', latest.latest_event_time, {{ now }}) as event_to_gold_s,
    last_hour.p50_ingest_latency_s,
    last_hour.p95_ingest_latency_s,
    last_hour.positions_last_hour
from latest
cross join last_hour
