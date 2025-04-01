WITH route_details AS (
    SELECT
        origin_airport.airport_name AS origin_airport_name,
        dest_airport.airport_name AS destination_airport_name,
        dim_route.route_key,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN fact_flights.is_departure_delayed = true THEN 1 ELSE 0 END) AS departure_delayed_flights,
        SUM(CASE WHEN fact_flights.is_arrival_delayed = true THEN 1 ELSE 0 END) AS arrival_delayed_flights,
        ROUND(
            (SUM(CASE WHEN fact_flights.is_departure_delayed = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*)),
            2
        ) AS departure_delay_percentage,
        ROUND(
            (SUM(CASE WHEN fact_flights.is_arrival_delayed = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*)),
            2
        ) AS arrival_delay_percentage
    FROM
        public.fact_flights fact_flights
    JOIN
        public.dim_route dim_route ON fact_flights.route_key = dim_route.route_key
    JOIN
        public.dim_airport origin_airport ON dim_route.origin = origin_airport.airport_code
    JOIN
        public.dim_airport dest_airport ON dim_route.destination = dest_airport.airport_code
    GROUP BY
        origin_airport.airport_name,
        dest_airport.airport_name,
        dim_route.route_key
)

SELECT
    origin_airport_name,
    destination_airport_name,
    total_flights,
    departure_delayed_flights,
    arrival_delayed_flights,
    departure_delay_percentage,
    arrival_delay_percentage
FROM
    route_details