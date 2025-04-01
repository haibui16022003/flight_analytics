WITH carrier_delays AS (
    SELECT
        dim_carrier.carrier_code,
        dim_carrier.carrier_name,
        COUNT(*) AS total_flights,
        COUNT(*) FILTER (WHERE fact_flights.is_departure_delayed = true) AS departure_delayed_flights,
        COUNT(*) FILTER (WHERE fact_flights.is_arrival_delayed = true) AS arrival_delayed_flights
    FROM
        public.fact_flights fact_flights
    JOIN
        public.dim_carrier dim_carrier ON fact_flights.carrier_code = dim_carrier.carrier_code
    GROUP BY
        dim_carrier.carrier_code, dim_carrier.carrier_name
)

SELECT
    carrier_code,
    carrier_name,
    total_flights,
    departure_delayed_flights,
    arrival_delayed_flights,
    ROUND((departure_delayed_flights * 100.0 / NULLIF(total_flights, 0)), 2) AS departure_delay_percentage,
    ROUND((arrival_delayed_flights * 100.0 / NULLIF(total_flights, 0)), 2) AS arrival_delay_percentage
FROM carrier_delays
ORDER BY carrier_name DESC