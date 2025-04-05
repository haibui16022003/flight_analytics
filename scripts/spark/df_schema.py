from pyspark.sql.types import IntegerType, FloatType, StringType

# Raw data schema
FLIGHTS_SCHEMA = {
    "year": IntegerType(),
    "month": IntegerType(),
    "day": IntegerType(),
    "dep_time": IntegerType(),
    "sched_dep_time": IntegerType(),
    "dep_delay": IntegerType(),
    "arr_time": IntegerType(),
    "sched_arr_time": IntegerType(),
    "arr_delay": IntegerType(),
    "carrier": StringType(),
    "flight": IntegerType(),
    "tailnum": StringType(),
    "origin": StringType(),
    "dest": StringType(),
    "air_time": IntegerType(),
    "distance": IntegerType(),
    "hour": IntegerType(),
    "minute": IntegerType(),
    "time_hour": StringType(),
    "name": StringType()
}

DELAYS_SCHEMA = {
    "year": IntegerType(),
    "month": IntegerType(),
    "carrier": StringType(),
    "carrier_name": StringType(),
    "airport": StringType(),
    "airport_name": StringType(),
    "arr_flights": FloatType(),
    "arr_del15": FloatType(),
    "carrier_ct": FloatType(),
    "weather_ct": FloatType(),
    "nas_ct": FloatType(),
    "security_ct": FloatType(),
    "late_aircraft_ct": FloatType(),
    "arr_cancelled": FloatType(),
    "arr_diverted": FloatType(),
    "arr_delay": FloatType(),
    "carrier_delay": FloatType(),
    "weather_delay": FloatType(),
    "nas_delay": FloatType(),
    "security_delay": FloatType(),
    "late_aircraft_delay": FloatType()
}
