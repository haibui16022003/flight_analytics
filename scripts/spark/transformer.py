from pyspark.sql.functions import col, to_date, concat_ws, sha2, date_format, lit, when, dayofweek

from normarlizer import load_csv

def transform_flight_data(spark, flights_df):
    """Transform the flights data into dimensional model."""

    def create_dim_carrier():
        df = flights_df.select(
            col("carrier").alias("carrier_code"),
            col("name").alias("carrier_name")
        )
        return df.distinct()

    def create_dim_airport():
        data_path = "/opt/data/"
        airport_df = load_csv(spark, data_path, "airports.csv")
        airport_df = airport_df.filter(col("iata_code").isNotNull())
        df = airport_df.select(
            col("iata_code").cast("string").alias("airport_code"),
            col("name").cast("string").alias("airport_name"),
            col("iso_country").cast("string").alias("country"),
            col("municipality").cast("string").alias("city")
        )
        return df.distinct()

    def create_dim_date():
        df = flights_df.select(
            col("year"),
            col("month"),
            col("day"),
        ).distinct()

        # Date format: yyyy-MM-dd
        df = df.withColumn("date", to_date(concat_ws("-", col("year"), col("month"), col("day")), "yyyy-M-d"))
        df = df.withColumn("date_code", sha2(concat_ws("", col("year"), col("month"), col("day")), 256))

        # Add additional date dimensions
        df = df.withColumn("day_of_week", date_format(col("date"), "EEEE"))
        df = df.withColumn("day_of_month", col("day"))
        df = df.withColumn("day_of_year", date_format(col("date"), "D"))
        df = df.withColumn("month_name", date_format(col("date"), "MMMM"))
        df = df.withColumn("quarter", date_format(col("date"), "Q"))
        df = df.withColumn("is_weekend", when(dayofweek(col("date")).isin(1, 7), True).otherwise(False))

        return df

    def create_dim_route():
        df = flights_df.select(
            col("origin"),
            col("dest").alias("destination"),
            col("distance")
        ).distinct()

        df = df.withColumn(
            "route_key",
            sha2(concat_ws("", col("origin"), col("destination"), col("distance")), 256)
        )
        return df

    def create_dim_time():
        df = flights_df.select(
            col("hour"),
            col("minute")
        ).distinct()

        df = df.withColumn("time_id", (col("hour") * 60 + col("minute")).cast("int"))
        df = df.withColumn("time_of_day",
                           when((col("hour") >= 5) & (col("hour") < 12), "Morning")
                           .when((col("hour") >= 12) & (col("hour") < 17), "Afternoon")
                           .when((col("hour") >= 17) & (col("hour") < 21), "Evening")
                           .otherwise("Night"))

        df = df.withColumn("formatted_time",
                           concat_ws(":",
                                     when(col("hour") < 10, concat_ws("", lit("0"), col("hour"))).otherwise(
                                         col("hour").cast("string")),
                                     when(col("minute") < 10, concat_ws("", lit("0"), col("minute"))).otherwise(
                                         col("minute").cast("string"))))

        return df

    # Create all dimensional tables
    dim_carrier = create_dim_carrier()
    dim_airport = create_dim_airport()
    dim_date = create_dim_date()
    dim_route = create_dim_route()
    dim_time = create_dim_time()

    def create_fact_flights():
       df = flights_df.withColumn("route_key",
                                  sha2(concat_ws("", col("origin"), col("dest"), col("distance")), 256))
       df = df.withColumn("date_code",
                          sha2(concat_ws("", col("year"), col("month"), col("day")), 256))
       df = df.withColumn("time_id", (col("hour") * 60 + col("minute")).cast("int"))
       df = df.withColumn("is_departure_delayed", when(col("dep_delay") > 15, True).otherwise(False))
       df = df.withColumn("is_arrival_delayed", when(col("arr_delay") > 15, True).otherwise(False))

       fact_df = df.select(
           col("tailnum").alias("tail_number"),
           col("carrier").alias("carrier_code"),
           col("flight").alias("flight_number"),
           col("air_time").alias("air_time"),
           col("route_key"),
           col("date_code"),
           col("time_id"),
           col("is_departure_delayed"),
           col("is_arrival_delayed")
       )

       return fact_df

    fact_flights = create_fact_flights()

    # Return dictionary of all dimensional tables
    return dim_carrier, dim_airport, dim_date, dim_route, dim_time, fact_flights

def transform_flight_delay(spark, delays_df):
    """Transform the delay reasons."""
    df = delays_df.select(
        col("year"),
        col("month"),
        col("carrier").alias("carrier_code"),
        col("airport").alias("airport_code"),
        col("arr_flights").alias("total_arrivals"),
        col("arr_del15").alias("total_delays"),
        col("carrier_ct").alias("carrier_cause_delay"),
        col("weather_ct").alias("weather_cause_delay"),
        col("nas_ct").alias("nas_cause_delay"),
        col("security_ct").alias("security_cause_delay"),
        col("late_aircraft_ct").alias("late_aircraft_cause_delay"),
        col("arr_cancelled").alias("total_cancellations"),
        col("arr_diverted").alias("total_diversions"),
    )

    return df
