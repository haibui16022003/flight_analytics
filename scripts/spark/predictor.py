from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import os
import time
import json
from datetime import datetime

from preprocessing import preprocess_data, split_data
from classification import build_model, evaluate_models, save_model, load_model
from transformer import transform_flight_data

# Create logs directory if it doesn't exist
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Create models directory if it doesn't exist
models_dir = "models"
os.makedirs(models_dir, exist_ok=True)

# Generate timestamp for log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"{log_dir}/flight_delay_prediction_{timestamp}.log"
img_prefix = f"{log_dir}/{timestamp}_"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="Flight Delay Predictor", config=None):
    """
    Create and return a SparkSession with optional configuration.

    Args:
        app_name: Name of the Spark application
        config: Dictionary of configuration parameters

    Returns:
        SparkSession object
    """
    builder = SparkSession.builder.appName(app_name)

    if config:
        for key, value in config.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()


def predict(model, sample_data, spark):
    """
    Make predictions using a trained model.

    Args:
        model: Trained pipeline model
        sample_data: Dictionary containing feature values
        spark: SparkSession object

    Returns:
        DataFrame: Prediction results
    """
    # Convert sample data to DataFrame
    sample_df = spark.createDataFrame([sample_data])

    # Make prediction
    try:
        prediction = model.transform(sample_df)
        prediction.select("prediction", "probability").show(truncate=False)
        return prediction
    except Exception as e:
        logger.error(f"Error making prediction: {str(e)}")
        raise


def visualize_results(model_results, model_type, training_times=None):
    """
    Visualize model evaluation results and training times.

    Args:
        model_results: List of tuples (model_name, metrics)
        model_type: String indicating the type of models (e.g., "arrival" or "departure")
        training_times: Dictionary of model training times
    """
    # Convert results to DataFrame for visualization
    results_df = pd.DataFrame([
        {
            "model": name,
            "metric": metric_name,
            "value": metric_value
        }
        for name, metrics in model_results
        for metric_name, metric_value in metrics.items()
        if metric_name not in ["evaluation_time"]  # Exclude timing info from performance metrics
    ])

    # Create performance visualization
    plt.figure(figsize=(12, 6))
    chart = sns.barplot(x="model", y="value", hue="metric", data=results_df)
    chart.set_title(f"{model_type.capitalize()} Delay Model Performance Comparison")
    chart.set_ylabel("Score")
    chart.set_xlabel("Model")
    plt.tight_layout()

    # Save to logs directory with timestamp
    performance_file = f"{img_prefix}{model_type}_model_performance.png"
    plt.savefig(performance_file)
    logger.info(f"{model_type.capitalize()} performance visualization saved to {performance_file}")
    plt.close()

    # Create timing visualization if timing data is provided
    if training_times:
        # Create DataFrame for timing info
        timing_data = []
        for name, metrics in model_results:
            timing_data.append({
                "model": name,
                "metric": "Training Time",
                "value": training_times[name]
            })
            timing_data.append({
                "model": name,
                "metric": "Evaluation Time",
                "value": metrics["evaluation_time"]
            })

        timing_df = pd.DataFrame(timing_data)

        # Create timing visualization
        plt.figure(figsize=(10, 5))
        chart = sns.barplot(x="model", y="value", hue="metric", data=timing_df)
        chart.set_title(f"{model_type.capitalize()} Model Training and Evaluation Times")
        chart.set_ylabel("Time (seconds)")
        chart.set_xlabel("Model")
        plt.tight_layout()

        # Save timing visualization
        timing_file = f"{img_prefix}{model_type}_model_timing.png"
        plt.savefig(timing_file)
        logger.info(f"{model_type.capitalize()} timing visualization saved to {timing_file}")
        plt.close()

    # Save results as JSON for future reference
    results_data = {
        name: {**metrics}
        for name, metrics in model_results
    }
    if training_times:
        for name in results_data:
            results_data[name]["training_time"] = training_times[name]

    with open(f"{log_dir}/{timestamp}_{model_type}_results.json", "w") as f:
        json.dump(results_data, f, indent=2)


def main():
    """Main execution function."""
    total_start_time = time.time()

    try:
        # Create Spark session with optimized configuration for worker utilization
        spark_config = {
            "spark.executor.memory": "2g",  # Memory per executor
            "spark.executor.cores": "2",  # Cores per executor
            "spark.executor.instances": "3",  # One executor per worker
            "spark.driver.memory": "4g",
            "spark.dynamicAllocation.enabled": "false",  # Disable dynamic allocation
            "spark.sql.shuffle.partitions": "100",
            "spark.default.parallelism": "100",
            "spark.scheduler.mode": "FAIR",  # Fair scheduling mode
            "spark.locality.wait": "0s"  # Don't wait for data locality
        }
        spark = create_spark_session(config=spark_config)
        logger.info("Spark session created successfully")

        # Log Spark application info for monitoring
        logger.info("Spark application info:")
        logger.info(f"Application ID: {spark.sparkContext.applicationId}")
        logger.info(f"Web UI URL: {spark.sparkContext.uiWebUrl}")

        # Benchmark data preprocessing
        logger.info("Processing flight data...")
        preprocessing_start = time.time()
        model_arr_df, model_dep_df = preprocess_data(spark, transform_flight_data)
        preprocessing_time = time.time() - preprocessing_start
        logger.info(f"Data preprocessing completed in {preprocessing_time:.2f} seconds")

        # Repartition to ensure data is distributed across workers
        num_partitions = 12  # 3 workers × 4 partitions per worker
        model_arr_df = model_arr_df.repartition(num_partitions)
        model_dep_df = model_dep_df.repartition(num_partitions)

        logger.info(f"Processed data: {model_arr_df.count()} arrival records, {model_dep_df.count()} departure records")

        # Cache datasets for better performance during model training
        model_arr_df.cache()
        model_dep_df.cache()

        # Split data into training and test sets
        logger.info("Splitting data into training and test sets...")
        split_start = time.time()
        train_arr_df, test_arr_df = split_data(model_arr_df, "is_arrival_delayed")
        train_dep_df, test_dep_df = split_data(model_dep_df, "is_departure_delayed")
        split_time = time.time() - split_start
        logger.info(f"Data splitting completed in {split_time:.2f} seconds")

        # Build and evaluate models for arrival delays
        logger.info("Building arrival delay prediction models (RandomForest and GBT only)...")
        arrival_models, arrival_training_times = build_model(train_arr_df, "is_arrival_delayed")

        logger.info("Evaluating arrival delay prediction models...")
        arrival_results = evaluate_models(arrival_models, test_arr_df, "is_arrival_delayed")

        # Build and evaluate models for departure delays
        logger.info("Building departure delay prediction models (RandomForest and GBT only)...")
        departure_models, departure_training_times = build_model(train_dep_df, "is_departure_delayed")

        logger.info("Evaluating departure delay prediction models...")
        departure_results = evaluate_models(departure_models, test_dep_df, "is_departure_delayed")

        # Visualize results
        logger.info("Visualizing results...")
        visualize_results(arrival_results, "arrival", arrival_training_times)
        visualize_results(departure_results, "departure", departure_training_times)

        # Find best models
        best_arrival_model_name = max(arrival_results, key=lambda x: x[1]["auc"])[0]
        best_departure_model_name = max(departure_results, key=lambda x: x[1]["auc"])[0]

        best_arrival_metrics = max(arrival_results, key=lambda x: x[1]["auc"])[1]
        best_departure_metrics = max(departure_results, key=lambda x: x[1]["auc"])[1]

        logger.info(f"Best arrival delay model: {best_arrival_model_name} with AUC {best_arrival_metrics['auc']:.4f}")
        logger.info(
            f"Best departure delay model: {best_departure_model_name} with AUC {best_departure_metrics['auc']:.4f}")

        # Save the best models
        logger.info("Saving best models...")

        # Get paths for models with timestamp and prefix to avoid overwriting
        arrival_model_path = f"{models_dir}/{timestamp}_arrival_{best_arrival_model_name}"
        departure_model_path = f"{models_dir}/{timestamp}_departure_{best_departure_model_name}"

        # Save models
        best_arrival_model = arrival_models[best_arrival_model_name]
        best_departure_model = departure_models[best_departure_model_name]

        arrival_saved = save_model(best_arrival_model, arrival_model_path)
        departure_saved = save_model(best_departure_model, departure_model_path)

        # Save model metadata
        if arrival_saved:
            arrival_model_metadata = {
                "model_type": best_arrival_model_name,
                "model_path": arrival_model_path,
                "timestamp": timestamp,
                "metrics": best_arrival_metrics,
                "training_time": arrival_training_times[best_arrival_model_name]
            }

            with open(f"{models_dir}/{timestamp}_arrival_model_metadata.json", "w") as f:
                json.dump(arrival_model_metadata, f, indent=2)
            logger.info(f"Arrival model metadata saved")

        if departure_saved:
            departure_model_metadata = {
                "model_type": best_departure_model_name,
                "model_path": departure_model_path,
                "timestamp": timestamp,
                "metrics": best_departure_metrics,
                "training_time": departure_training_times[best_departure_model_name]
            }

            with open(f"{models_dir}/{timestamp}_departure_model_metadata.json", "w") as f:
                json.dump(departure_model_metadata, f, indent=2)
            logger.info(f"Departure model metadata saved")

        # Log training time summary
        logger.info("Training time summary:")
        for model_name, train_time in arrival_training_times.items():
            logger.info(f"  Arrival {model_name}: {train_time:.2f} seconds")
        for model_name, train_time in departure_training_times.items():
            logger.info(f"  Departure {model_name}: {train_time:.2f} seconds")

        # Log total execution time
        total_time = time.time() - total_start_time
        logger.info(f"Total execution time: {total_time:.2f} seconds")

        # Save a summary of timing data
        timing_summary = {
            "preprocessing_time": preprocessing_time,
            "split_time": split_time,
            "arrival_training_times": arrival_training_times,
            "departure_training_times": departure_training_times,
            "total_time": total_time,
            "best_models": {
                "arrival": {
                    "name": best_arrival_model_name,
                    "path": arrival_model_path if arrival_saved else "Failed to save"
                },
                "departure": {
                    "name": best_departure_model_name,
                    "path": departure_model_path if departure_saved else "Failed to save"
                }
            }
        }

        with open(f"{log_dir}/{timestamp}_timing_summary.json", "w") as f:
            json.dump(timing_summary, f, indent=2)

        # Clean up
        spark.stop()
        logger.info("Processing completed successfully")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()