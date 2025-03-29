WITH monthly_delays AS (
    SELECT
        dim_date.year,
        dim_date.month,
        COUNT(*) AS total_flights,
        COUNT(*) FILTER (WHERE fact_flights.is_departure_delayed = true) AS departure_delayed_flights,
        COUNT(*) FILTER (WHERE fact_flights.is_arrival_delayed = true) AS arrival_delayed_flights
    FROM public.fact_flights fact_flights
    JOIN public.dim_date dim_date ON fact_flights.date_code = dim_date.date_code
    GROUP BY dim_date.year, dim_date.month
)

SELECT
    year,
    month,
    total_flights,
    departure_delayed_flights,
    arrival_delayed_flights,
    ROUND((departure_delayed_flights * 100.0 / NULLIF(total_flights, 0)), 2) AS departure_delay_percentage,
    ROUND((arrival_delayed_flights * 100.0 / NULLIF(total_flights, 0)), 2) AS arrival_delay_percentage
FROM monthly_delays
ORDER BY year, month
