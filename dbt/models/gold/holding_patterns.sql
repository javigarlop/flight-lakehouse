-- Flights that circled near their arrival airport before landing.
-- The turn between consecutive positions is the change in true_track, folded into
-- [-180, 180). A flight holds when its positions within the holding radius add up to at
-- least one full turn in the same direction. With live 4-minute polls the headings are too
-- sparse to see this, so the model is meant for replayed full-resolution recordings.
-- Flights that land where they took off are left out: those are local and training
-- flights flying circuits, not aircraft holding for their turn to land.

{{ config(unique_key="flight_id") }}

{% set holding_radius_km = 40 %}

with arriving as (
    select
        flights.flight_id,
        flights.icao24,
        flights.callsign,
        flights.arrival_airport,
        flights.first_seen,
        flights.last_seen,
        airports.latitude as airport_latitude,
        airports.longitude as airport_longitude
    from {{ source("silver", "flights") }} as flights
    inner join {{ ref("airports") }} as airports
        on flights.arrival_airport = airports.airport_code
    where
        flights.departure_airport is distinct from flights.arrival_airport
        {% if is_incremental() %}
            and flights.last_seen >= {{ lookback_start("landed_at") }}
        {% endif %}
),

turns as (
    select
        arriving.flight_id,
        positions.event_time,
        mod(
            positions.true_track
            - lag(positions.true_track)
                over (
                    partition by arriving.flight_id order by positions.event_time
                )
            + 540,
            360
        ) - 180 as turn_deg
    from arriving
    inner join {{ source("silver", "flight_positions") }} as positions
        on
            arriving.icao24 = positions.icao24
            and positions.event_time between arriving.first_seen and arriving.last_seen
    where
        positions.true_track is not null
        and not positions.on_ground
        and great_circle_distance(
            positions.latitude,
            positions.longitude,
            arriving.airport_latitude,
            arriving.airport_longitude
        ) <= {{ holding_radius_km }}
),

per_flight as (
    select
        flight_id,
        min(event_time) as holding_start,
        max(event_time) as holding_end,
        count(*) as positions,
        coalesce(sum(turn_deg), 0) as total_turn_deg
    from turns
    group by 1
)

select
    arriving.flight_id,
    arriving.icao24,
    arriving.callsign,
    arriving.arrival_airport as airport_code,
    per_flight.holding_start,
    per_flight.holding_end,
    arriving.last_seen as landed_at,
    per_flight.positions,
    per_flight.total_turn_deg,
    cast(floor(abs(per_flight.total_turn_deg) / 360) as integer) as laps
from per_flight
inner join arriving on per_flight.flight_id = arriving.flight_id
where abs(per_flight.total_turn_deg) >= 360
