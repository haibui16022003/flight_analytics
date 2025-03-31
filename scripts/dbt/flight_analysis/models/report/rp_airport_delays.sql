WITH departure_delays AS (
    SELECT
        r.origin AS airport_code,  -- Sân bay cất cánh
        COUNT(*) AS total_departures,
        COUNT(*) FILTER (WHERE f.is_departure_delayed) AS delayed_departures
    FROM public.fact_flights f
    JOIN public.dim_route r ON f.route_key = r.route_key
    GROUP BY r.origin
),

arrival_delays AS (
    SELECT
        r.destination AS airport_code,  -- Sân bay đến
        COUNT(*) AS total_arrivals,
        COUNT(*) FILTER (WHERE f.is_arrival_delayed) AS delayed_arrivals
    FROM public.fact_flights f
    JOIN public.dim_route r ON f.route_key = r.route_key
    GROUP BY r.destination
),

airport_info AS (
    SELECT
        airport_code,
        airport_name,
        city,
        country
    FROM public.dim_airport
)

SELECT
    a.airport_code,
    a.airport_name,
    a.city,
    a.country,

    COALESCE(d.total_departures, 0) AS total_departures,
    COALESCE(d.delayed_departures, 0) AS delayed_departures,
    ROUND((COALESCE(d.delayed_departures, 0) * 100.0) / NULLIF(d.total_departures, 0), 2) AS departure_delay_percentage,

    COALESCE(arr.total_arrivals, 0) AS total_arrivals,
    COALESCE(arr.delayed_arrivals, 0) AS delayed_arrivals,
    ROUND((COALESCE(arr.delayed_arrivals, 0) * 100.0) / NULLIF(arr.total_arrivals, 0), 2) AS arrival_delay_percentage

FROM airport_info a
LEFT JOIN departure_delays d ON a.airport_code = d.airport_code
LEFT JOIN arrival_delays arr ON a.airport_code = arr.airport_code
ORDER BY a.airport_name, a.city, a.country