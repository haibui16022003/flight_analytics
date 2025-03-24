from pyspark.sql import SparkSession

from normarlizer import normalize_data, print_schema_info
from df_schema import *
from transformer import *

POSTGRES_JAR = "/opt/spark/jars/postgresql-42.2.23.jar"


def create_spark_session(app_name="Loader"):
    """Create and return a SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", POSTGRES_JAR) \
        .getOrCreate()


def load_flights_data(spark, flights_df):
    """Load flights data into PostgreSQL."""
    dim_carrier, dim_airport, dim_date, dim_route, dim_time, fact_flights = transform_flight_data(spark, flights_df)
    tables = {
        "dim_carrier": dim_carrier,
        "dim_airport": dim_airport,
        "dim_date": dim_date,
        "dim_route": dim_route,
        "dim_time": dim_time,
        "fact_flights": fact_flights
    }

    # Postgre configuration
    jdbc_url = "jdbc:postgresql://postgresql:5432/flights_dwh"
    db_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    # Write data to PostgreSQL
    for table_name, df in tables.items():
        print(f"Loading {table_name} into PostgreSQL...")

        try:
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", db_properties["user"]) \
                .option("password", db_properties["password"]) \
                .option("driver", db_properties["driver"]) \
                .mode("overwrite") \
                .save()
            print(f"Successfully loaded {table_name}!")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")


def load_delays_data(spark, delays_df):
    """Load delays data into PostgreSQL."""
    try:
        delays_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgresql:5432/flights_dwh") \
            .option("dbtable", "delay_reasons") \
            .option("user", "user") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("Successfully loaded delay_reasons!")
    except Exception as e:
        print(f"Error loading delays_reasons: {e}")


if __name__ == "__main__":
    spark = create_spark_session("FlightsLoader")

    # # Load and normalize flights data
    flights_df = normalize_data(spark, "flight_data.csv", FLIGHTS_SCHEMA)
    print_schema_info(flights_df, show_sample=True)
    load_flights_data(spark, flights_df)

    # Normalize delays data
    delays_df = normalize_data(spark, "delay_cause.csv", DELAYS_SCHEMA)
    print_schema_info(delays_df, show_sample=True)
    load_delays_data(spark, delays_df)

    # Stop the SparkSession
    spark.stop()
