from pyspark.sql.functions import sum, round

def generate_delay_report(spark):
    """Generate and save delay report in PostgreSQL."""
    jdbc_url = "jdbc:postgresql://postgresql:5432/flights_dwh"
    db_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    query = """
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
    """

    # Load data from PostgreSQL
    delay_report_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"({query}) AS delay_report") \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .load()

    # Save report to PostgreSQL
    try:
        delay_report_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "delay_report") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .mode("overwrite") \
            .save()
        print("\033[92mSuccessfully generated delay report!\033[0m")
    except Exception as e:
        print("\033[91mError generating delay report!\033[0m", e)
