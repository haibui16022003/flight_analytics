from pyspark.sql import SparkSession

from normarlizer import normalize_data, print_schema_info
from df_schema import *
from transformer import *

DATA_PATH = "/opt/data/"
POSTGRES_JAR = "/opt/spark/jars/postgresql-42.2.23.jar"

def create_spark_session(app_name="Loader"):
    """Create and return a SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", POSTGRES_JAR) \
        .getOrCreate()


def load_flights_data(spark, df_names):
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
    jdbc_url = "jdbc:postgresql://flight_analytics-postgresql-1:5432/flights_dwh"
    db_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    # Write data to PostgreSQL
    for table_name, df in tables.items():
        print(f"Loading {table_name} into PostgreSQL...")

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


if __name__ == "__main__":
    spark = create_spark_session("FlightsLoader")
    flights_df = normalize_data(spark, "flight_data.csv", FLIGHTS_SCHEMA)
    print_schema_info(flights_df, show_sample=True)

    # Load data into PostgreSQL
    load_flights_data(spark, flights_df)
    list_df = transform_flight_data(spark, flights_df)
    print_schema_info(list_df[5], show_sample=True)
    # Stop the SparkSession
    spark.stop()

