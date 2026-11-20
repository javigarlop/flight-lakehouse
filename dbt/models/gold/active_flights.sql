-- Aircraft in the air right now: one row per aircraft, with its latest position.
-- A view, not a table: it reads silver when queried, so it is never more than one poll
-- behind, where a table rebuilt every 15 minutes would be up to 15 minutes stale.
-- Active means the latest position is under 10 minutes old (one missed 4-minute poll
-- allowed) and airborne; aircraft on the ground, such as parked ones still transmitting,
-- are left out. The flight starts after the last gap of 20 minutes or more, the rule
-- silver.flights uses, so flight_id matches the row silver.flights writes once it ends.
-- Replayed recordings have old event times, so they show no active flights.

{{ config(materialized="view") }}

{% set active_minutes = 10 %}
{% set session_gap_minutes = 20 %}
{% set now = "cast(current_timestamp as timestamp(6) with time zone)" %}

with recent as (
    -- Longer than any flight in the area, so its first position is always in range.
    select *
    from {{ source("silver", "flight_positions") }}
    where event_time between {{ now }} - interval '12' hour and {{ now }}
),

gaps as (
    select
        *,
        coalesce(
            event_time - lag(event_time) over (partition by icao24 order by event_time)
            >= interval '{{ session_gap_minutes }}' minute,
            true
        ) as starts_flight,
        row_number() over (partition by icao24 order by event_time desc) as recency
    from recent
),

flight_start as (
    select
        icao24,
        max(event_time) as first_seen
    from gaps
    where starts_flight
    group by 1
),

latest as (
    select * from gaps
    where recency = 1
)

select
    latest.icao24
    || '-'
    || format_datetime(flight_start.first_seen, 'yyyyMMdd''T''HHmmss') as flight_id,
    latest.icao24,
    latest.callsign,
    latest.registration,
    latest.aircraft_type,
    flight_start.first_seen,
    latest.event_time as last_seen,
    date_diff('second', flight_start.first_seen, latest.event_time) as duration_s,
    date_diff('second', latest.event_time, {{ now }}) as age_s,
    latest.latitude,
    latest.longitude,
    latest.baro_altitude,
    latest.geo_altitude,
    latest.velocity,
    latest.true_track,
    latest.vertical_rate,
    latest.squawk
from latest
inner join flight_start on latest.icao24 = flight_start.icao24
where
    latest.event_time >= {{ now }} - interval '{{ active_minutes }}' minute
    and not latest.on_ground
