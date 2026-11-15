-- Flights and duration per route and day (UTC, by first_seen).
-- Most flights enter or leave the polling area in cruise, so one end of the route is often
-- outside it: that end is null and the route reads e.g. 'LEBL-outside'. Durations are then
-- the time spent in the area, not the whole flight. Overflights with neither end known are
-- left out. Percentiles are exact (nearest rank), so a backfill reproduces them.

{{ config(unique_key=["flight_date", "route"]) }}

select
    cast(first_seen as date) as flight_date,
    coalesce(departure_airport, 'outside') || '-' || coalesce(arrival_airport, 'outside')
        as route,
    departure_airport,
    arrival_airport,
    count(*) as flights,
    {{ nearest_rank("duration_s", 0.5) }} as median_duration_s,
    {{ nearest_rank("duration_s", 0.25) }} as p25_duration_s,
    {{ nearest_rank("duration_s", 0.75) }} as p75_duration_s
from {{ source("silver", "flights") }}
where
    (departure_airport is not null or arrival_airport is not null)
    {% if is_incremental() %}
        and cast(first_seen as date) >= cast(
            {{ lookback_start("with_timezone(cast(flight_date as timestamp), 'UTC')") }} as date
        )
    {% endif %}
group by 1, 2, 3, 4
