WITH
    delay_report_data
    AS
    (
        SELECT
            year,
            month,
            carrier,
            carrier_name,
            airport,
            airport_name,
            SUM(arr_flights) AS total_flights,
            SUM(arr_del15) AS total_delayed_flights,
            SUM(arr_cancelled) AS total_cancelled_flights,
            SUM(arr_diverted) AS total_diverted_flights,
            SUM(carrier_ct) AS total_carrier_delays,
            SUM(weather_ct) AS total_weather_delays,
            SUM(nas_ct) AS total_nas_delays,
            SUM(security_ct) AS total_security_delays,
            SUM(late_aircraft_ct) AS total_late_aircraft_delays,
            SUM(arr_delay) AS total_delay_minutes,
            SUM(carrier_delay) AS total_carrier_delay_minutes,
            SUM(weather_delay) AS total_weather_delay_minutes,
            SUM(nas_delay) AS total_nas_delay_minutes,
            SUM(security_delay) AS total_security_delay_minutes,
            SUM(late_aircraft_delay) AS total_late_aircraft_delay_minutes
        FROM delay_reasons
        GROUP BY year, month, carrier, carrier_name, airport, airport_name
    )

SELECT
    year,
    month,
    carrier,
    carrier_name,
    airport,
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
    total_delay_minutes,
    total_carrier_delay_minutes,
    total_weather_delay_minutes,
    total_nas_delay_minutes,
    total_security_delay_minutes,
    total_late_aircraft_delay_minutes
FROM delay_report_data