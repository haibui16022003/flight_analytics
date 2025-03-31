WITH
    delay_report_data_by_airport
    AS
    (
        SELECT
            dr.airport_code,
            a.airport_name,
            SUM(dr.total_arrivals) AS total_flights,
            SUM(dr.total_delays) AS total_delayed_flights,
            SUM(dr.total_cancellations) AS total_cancelled_flights,
            SUM(dr.total_diversions) AS total_diverted_flights,
            SUM(dr.carrier_cause_delay) AS total_carrier_delays,
            SUM(dr.weather_cause_delay) AS total_weather_delays,
            SUM(dr.nas_cause_delay) AS total_nas_delays,
            SUM(dr.security_cause_delay) AS total_security_delays,
            SUM(dr.late_aircraft_cause_delay) AS total_late_aircraft_delays
        FROM public.delay_reasons dr
        JOIN public.dim_airport a ON dr.airport_code = a.airport_code
        GROUP BY dr.airport_code, a.airport_name
    )

SELECT
    airport_code,
    airport_name,
    total_flights,
    total_delayed_flights,
    total_cancelled_flights,
    total_diverted_flights,
    total_carrier_delays,
    total_weather_delays,
    total_nas_delays,
    total_security_delays,
    total_late_aircraft_delays,
    (total_delayed_flights / NULLIF(total_flights, 0) * 100)::DECIMAL(5,2) AS delay_percentage,
    (total_carrier_delays / NULLIF(total_delayed_flights, 0) * 100)::DECIMAL(5,2) AS carrier_delay_percentage,
    (total_weather_delays / NULLIF(total_delayed_flights, 0) * 100)::DECIMAL(5,2) AS weather_delay_percentage,
    (total_nas_delays / NULLIF(total_delayed_flights, 0) * 100)::DECIMAL(5,2) AS nas_delay_percentage,
    (total_security_delays / NULLIF(total_delayed_flights, 0) * 100)::DECIMAL(5,2) AS security_delay_percentage,
    (total_late_aircraft_delays / NULLIF(total_delayed_flights, 0) * 100)::DECIMAL(5,2) AS late_aircraft_delay_percentage
FROM delay_report_data_by_airport
ORDER BY total_delayed_flights DESC