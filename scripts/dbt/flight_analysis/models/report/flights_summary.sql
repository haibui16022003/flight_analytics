SELECT
  f.flight_number,
  f.tail_number,
  c.carrier_code,
  c.carrier_name,
  d.date,
  d.day_of_week,
  d.month,
  d.year,
  d.quarter,
  r.origin,
  r.destination,
  r.distance,
  t.formatted_time,
  t.time_of_day,
  f.is_departure_delayed,
  f.is_arrival_delayed
FROM public.fact_flights f
JOIN public.dim_carrier c ON f.carrier_code = c.carrier_code
JOIN public.dim_date d ON f.date_code = d.date_code
JOIN public.dim_route r ON f.route_key = r.route_key
JOIN public.dim_time t ON f.time_id = t.time_id