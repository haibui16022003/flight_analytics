import os
from pyspark.sql import SparkSession

from normarlizer import normalize_data, print_schema_info
from df_schema import *
from transformer import *

POSTGRES_JAR = "/opt/spark/jars/postgresql-42.2.23.jar"

# Get PostgreSQL connection info from environment variables
JDBC_HOST = os.getenv("JDBC_HOST")
JDBC_PORT = os.getenv("JDBC_PORT")
JDBC_DB = os.getenv("JDBC_DB")
JDBC_USER = os.getenv("JDBC_USER")
JDBC_PASSWORD = os.getenv("JDBC_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{JDBC_HOST}:{JDBC_PORT}/{JDBC_DB}"
DB_PROPERTIES = {
    "user": JDBC_USER,
    "password": JDBC_PASSWORD,
    "driver": "org.postgresql.Driver"
}


def create_spark_session(app_name="Loader"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", POSTGRES_JAR) \
        .getOrCreate()


def load_flights_data(spark, flights_df):
    dim_carrier, dim_airport, dim_date, dim_route, dim_time, fact_flights = transform_flight_data(spark, flights_df)
    tables = {
        "dim_carrier": dim_carrier,
        "dim_airport": dim_airport,
        "dim_date": dim_date,
        "dim_route": dim_route,
        "dim_time": dim_time,
        "fact_flights": fact_flights
    }

    for table_name, df in tables.items():
        print(f"Loading {table_name} into PostgreSQL...")
        try:
            df.write \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", table_name) \
                .option("user", DB_PROPERTIES["user"]) \
                .option("password", DB_PROPERTIES["password"]) \
                .option("driver", DB_PROPERTIES["driver"]) \
                .mode("overwrite") \
                .save()
            print(f"Successfully loaded {table_name}!")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")


def load_delays_data(spark, delays_df):
    try:
        delays_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "delay_reasons") \
            .option("user", DB_PROPERTIES["user"]) \
            .option("password", DB_PROPERTIES["password"]) \
            .option("driver", DB_PROPERTIES["driver"]) \
            .mode("overwrite") \
            .save()
        print("Successfully loaded delay_reasons!")
    except Exception as e:
        print(f"Error loading delay_reasons: {e}")


if __name__ == "__main__":
    spark = create_spark_session("FlightsLoader")

    # Normalize and load flights data
    flights_df = normalize_data(spark, "flight_data.csv", FLIGHTS_SCHEMA)
    print_schema_info(flights_df, show_sample=True)
    load_flights_data(spark, flights_df)

    # Normalize and load delays data
    delays_df = normalize_data(spark, "delay_cause.csv", DELAYS_SCHEMA)
    delays_df = transform_flight_delay(spark, delays_df)
    print_schema_info(delays_df, show_sample=True)
    load_delays_data(spark, delays_df)

    spark.stop()
