from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.types import IntegerType, StringType, DoubleType

def load_csv(spark, data_path, file_name, header=True, infer_schema=True):
    """Load data from CSV file."""
    full_path = data_path + file_name
    print(f"Attempting to read from path: {full_path}")
    return spark.read.csv(full_path, header=header, inferSchema=infer_schema)


def print_schema_info(df, show_sample=False):
    """Print schema information and optionally show a sample of the data."""
    print("Schema information:")
    df.printSchema()

    print("\nDetails column information:")
    for column in df.columns:
        print(f"{column}: {df.select(column).dtypes}")

    if show_sample:
        print("\nSample data:")
        df.show(5, truncate=False)


def handle_null_values(df):
    """
    Handle null values in the DataFrame:
    - Replace nulls in integer columns with 0
    - Replace nulls in string columns with "Unknown"
    - Replace nulls in double columns with 0.0

    Returns a new DataFrame with null values replaced.
    """
    schema = df.schema

    # Process each column based on its data type
    for field in schema.fields:
        column_name = field.name
        data_type = field.dataType

        # Handle integer columns
        if isinstance(data_type, IntegerType):
            df = df.withColumn(column_name, coalesce(col(column_name), lit(0)))

        # Handle string columns
        elif isinstance(data_type, StringType):
            df = df.withColumn(column_name, coalesce(col(column_name), lit("Unknown")))

        # Handle double columns
        elif isinstance(data_type, DoubleType):
            df = df.withColumn(column_name, coalesce(col(column_name), lit(0.0)))

    return df


def normalize_data(spark, file_name, schema: dict):
    """Load and normalize flights data using the predefined schema."""
    # if spark is None:
    #     spark = create_spark_session("FlightsNormalizer")
    data_path = "/opt/data/"
    df = load_csv(spark, data_path, file_name)

    for column, data_type in schema.items():
        df = df.withColumn(column, col(column).cast(data_type))

    df = handle_null_values(df)
    return df