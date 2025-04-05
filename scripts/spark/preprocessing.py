from pyspark.sql.functions import col


def enrich_data(spark, transform_flight_data):
    """
    Enrich flight data by joining fact and dimension tables.

    Args:
        spark: SparkSession object
        transform_flight_data: Function that transforms raw flight data

    Returns:
        DataFrame: Enriched flight data
    """
    dim_carrier, dim_airport, dim_date, dim_route, dim_time, fact_flights = transform_flight_data(spark)

    # Join fact table with dimension tables
    enriched_df = fact_flights \
        .join(dim_date, on="date_code", how="left") \
        .join(dim_route, on="route_key", how="left") \
        .join(dim_time, on="time_id", how="left")

    # Optionally join with other dimension tables if needed
    # .join(dim_carrier, on="carrier_code", how="left") \
    # .join(dim_airport, fact_flights["origin"] == dim_airport["airport_code"], how="left")

    return enriched_df


def preprocess_data(spark, transform_flight_data):
    """
    Preprocess flight data for modeling.

    Args:
        spark: SparkSession object
        transform_flight_data: Function that transforms raw flight data

    Returns:
        tuple: (arrival_delay_model_df, departure_delay_model_df)
    """
    enriched_df = enrich_data(spark, transform_flight_data)

    # Define common features for both models
    feature_cols = [
        "carrier_code", "origin", "destination", "distance", "air_time",
        "day_of_week", "is_weekend", "day_of_month", "month", "time_of_day"
    ]

    # Prepare dataset for arrival delay prediction
    model_arr_df = enriched_df.select(*feature_cols, "is_arrival_delayed") \
        .withColumn("is_arrival_delayed", col("is_arrival_delayed").cast("double")) \
        .withColumn("is_weekend", col("is_weekend").cast("double"))

    # Prepare dataset for departure delay prediction
    model_dep_df = enriched_df.select(*feature_cols, "is_departure_delayed") \
        .withColumn("is_departure_delayed", col("is_departure_delayed").cast("double")) \
        .withColumn("is_weekend", col("is_weekend").cast("double"))

    # Drop rows with null values
    return model_arr_df.na.drop(), model_dep_df.na.drop()


def split_data(df, label_col, train_ratio=0.8, seed=42):
    """
    Split the DataFrame into training and test sets.

    Args:
        df: DataFrame to split
        label_col: Name of the label column
        train_ratio: Ratio of training data (default: 0.8)
        seed: Random seed for reproducibility (default: 42)

    Returns:
        tuple: (train_df, test_df)
    """
    # Ensure the label column is included in both datasets
    feature_cols = [col for col in df.columns if col != label_col]
    columns_to_select = feature_cols + [label_col]

    # Split the data
    train_df, test_df = df.randomSplit([train_ratio, 1.0 - train_ratio], seed=seed)

    return train_df.select(*columns_to_select), test_df.select(*columns_to_select)