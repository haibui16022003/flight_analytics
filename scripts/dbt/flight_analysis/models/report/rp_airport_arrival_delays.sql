WITH arrival_delays AS (
    SELECT
        r.destination AS airport_code,
        COUNT(*) AS total_arrivals,
        COUNT(*) FILTER (WHERE f.is_arrival_delayed) AS delayed_arrivals
    FROM public.fact_flights f
    JOIN public.dim_route r ON f.route_key = r.route_key
    GROUP BY r.destination
)

SELECT
    a.airport_code,
    a.airport_name,
    a.city,
    a.country,
    arr.total_arrivals,
    arr.delayed_arrivals,
    ROUND((arr.delayed_arrivals * 100.0) / NULLIF(arr.total_arrivals, 0), 2) AS arrival_delay_percentage
FROM public.dim_airport a
INNER JOIN arrival_delays arr ON a.airport_code = arr.airport_code