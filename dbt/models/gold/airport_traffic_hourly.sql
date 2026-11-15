-- Arrivals, departures and aircraft on the ground per airport and hour (UTC).
-- Departures and arrivals come from the airports silver matched for each flight. Aircraft
-- on the ground are the distinct aircraft that reported on_ground near the airport.

{{ config(unique_key=["airport_code", "hour_start"]) }}

{% set on_ground_radius_km = 5 %}

with flights as (
    select * from {{ source("silver", "flights") }}
),

departures as (
    select
        departure_airport as airport_code,
        date_trunc('hour', first_seen) as hour_start,
        count(*) as departures
    from flights
    where
        departure_airport is not null
        {% if is_incremental() %}
            and date_trunc('hour', first_seen) >= {{ lookback_start("hour_start") }}
        {% endif %}
    group by 1, 2
),

arrivals as (
    select
        arrival_airport as airport_code,
        date_trunc('hour', last_seen) as hour_start,
        count(*) as arrivals
    from flights
    where
        arrival_airport is not null
        {% if is_incremental() %}
            and date_trunc('hour', last_seen) >= {{ lookback_start("hour_start") }}
        {% endif %}
    group by 1, 2
),

on_ground as (
    select
        airports.airport_code,
        date_trunc('hour', positions.event_time) as hour_start,
        count(distinct positions.icao24) as aircraft_on_ground
    from {{ source("silver", "flight_positions") }} as positions
    inner join {{ ref("airports") }} as airports
        on
            great_circle_distance(
                positions.latitude, positions.longitude, airports.latitude, airports.longitude
            ) <= {{ on_ground_radius_km }}
    where
        positions.on_ground
        {% if is_incremental() %}
            and date_trunc('hour', positions.event_time) >= {{ lookback_start("hour_start") }}
        {% endif %}
    group by 1, 2
)

select
    coalesce(departures.airport_code, arrivals.airport_code, on_ground.airport_code)
        as airport_code,
    coalesce(departures.hour_start, arrivals.hour_start, on_ground.hour_start) as hour_start,
    coalesce(departures.departures, 0) as departures,
    coalesce(arrivals.arrivals, 0) as arrivals,
    coalesce(on_ground.aircraft_on_ground, 0) as aircraft_on_ground
from departures
full outer join arrivals
    on
        departures.airport_code = arrivals.airport_code
        and departures.hour_start = arrivals.hour_start
full outer join on_ground
    on
        coalesce(departures.airport_code, arrivals.airport_code) = on_ground.airport_code
        and coalesce(departures.hour_start, arrivals.hour_start) = on_ground.hour_start
