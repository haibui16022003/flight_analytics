WITH delay_reasons AS (
    SELECT
        year,
        month,
        carrier_code,
        airport_code,
        total_arrivals,
        total_delays,
        carrier_cause_delay,
        weather_cause_delay,
        nas_cause_delay,
        security_cause_delay,
        late_aircraft_cause_delay,
        total_cancellations,
        total_diversions
    FROM public.delay_reasons
),

dim_carrier AS (
    SELECT
        carrier_code,
        carrier_name
    FROM public.dim_carrier
),

dim_airport AS (
    SELECT
        airport_code,
        airport_name,
        city,
        country
    FROM public.dim_airport
),

joined AS (
    SELECT
        dr.year,
        dr.month,
        dr.carrier_code,
        dc.carrier_name,
        dr.airport_code,
        da.airport_name,
        da.city,
        da.country,

        dr.total_arrivals,
        dr.total_delays,

        dr.carrier_cause_delay,
        dr.weather_cause_delay,
        dr.nas_cause_delay,
        dr.security_cause_delay,
        dr.late_aircraft_cause_delay,

        dr.total_cancellations,
        dr.total_diversions
    FROM delay_reasons dr
    JOIN dim_carrier dc ON dr.carrier_code = dc.carrier_code
    JOIN dim_airport da ON dr.airport_code = da.airport_code
)

SELECT * FROM joined
