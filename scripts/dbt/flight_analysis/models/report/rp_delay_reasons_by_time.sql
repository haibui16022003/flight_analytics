WITH
    delay_report_data_by_time
    AS
    (
        SELECT
            year,
            month,
            SUM(total_arrivals) AS total_flights,
            SUM(total_delays) AS total_delayed_flights,
            SUM(total_cancellations) AS total_cancelled_flights,
            SUM(total_diversions) AS total_diverted_flights,
            SUM(carrier_cause_delay) AS total_carrier_delays,
            SUM(weather_cause_delay) AS total_weather_delays,
            SUM(nas_cause_delay) AS total_nas_delays,
            SUM(security_cause_delay) AS total_security_delays,
            SUM(late_aircraft_cause_delay) AS total_late_aircraft_delays
        FROM {{ ref('delay_reasons') }}
        GROUP BY year, month
    )

SELECT
    year,
    month,
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
FROM delay_report_data_by_time
ORDER BY year, month