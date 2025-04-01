WITH departure_delays AS (
    SELECT
        r.origin AS airport_code,
        COUNT(*) AS total_departures,
        COUNT(*) FILTER (WHERE f.is_departure_delayed) AS delayed_departures
    FROM public.fact_flights f
    JOIN public.dim_route r ON f.route_key = r.route_key
    GROUP BY r.origin
)

SELECT
    a.airport_code,
    a.airport_name,
    a.city,
    a.country,
    d.total_departures,
    d.delayed_departures,
    ROUND((d.delayed_departures * 100.0) / NULLIF(d.total_departures, 0), 2) AS departure_delay_percentage
FROM public.dim_airport a
INNER JOIN departure_delays d ON a.airport_code = d.airport_code