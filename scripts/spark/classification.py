from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import time
import logging
import os

logger = logging.getLogger(__name__)


def build_feature_pipeline(categorical_cols, numerical_cols):
    """
    Build a feature processing pipeline for categorical and numerical features.

    Args:
        categorical_cols: List of categorical column names
        numerical_cols: List of numerical column names

    Returns:
        tuple: (pipeline_stages, feature_cols) containing preprocessing stages and resulting feature column names
    """
    pipeline_stages = []
    transformed_categorical_cols = []

    # Process categorical columns: indexing and one-hot encoding
    for col_name in categorical_cols:
        # Handle invalid values with 'keep' strategy to avoid runtime errors
        indexer = StringIndexer(
            inputCol=col_name,
            outputCol=f"{col_name}_idx",
            handleInvalid="keep"
        )
        encoder = OneHotEncoder(
            inputCols=[f"{col_name}_idx"],
            outputCols=[f"{col_name}_ohe"]
        )
        pipeline_stages += [indexer, encoder]
        transformed_categorical_cols.append(f"{col_name}_ohe")

    # Combine transformed categorical and numerical columns
    feature_cols = transformed_categorical_cols + numerical_cols

    return pipeline_stages, feature_cols


def build_model(train_df, label_col, cv_folds=3):
    """
    Build and train RandomForest and GBT classification models with cross-validation.

    Args:
        train_df: Training DataFrame
        label_col: Name of the label column
        cv_folds: Number of cross-validation folds

    Returns:
        dict: Dictionary of trained models and training times
    """
    # Define feature columns
    categorical_cols = [
        "carrier_code", "origin", "destination", "day_of_week", "month", "time_of_day"
    ]
    numerical_cols = [
        "distance", "air_time", "day_of_month", "is_weekend"
    ]

    # Build feature pipeline
    feature_stages, feature_cols = build_feature_pipeline(categorical_cols, numerical_cols)
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Define only RandomForest and GBT models
    models = {
        "RandomForest": RandomForestClassifier(labelCol=label_col, featuresCol="features", numTrees=100),
        "GBT": GBTClassifier(labelCol=label_col, featuresCol="features", maxIter=50)
    }

    trained_models = {}
    training_times = {}

    for name, model in models.items():
        logger.info(f"Training {name}...")

        # Create complete pipeline
        pipeline = Pipeline(stages=feature_stages + [assembler, model])

        # Create parameter grid for tuning
        if name == "RandomForest":
            param_grid = ParamGridBuilder() \
                .addGrid(model.maxDepth, [5, 10]) \
                .addGrid(model.numTrees, [50, 100]) \
                .build()
        else:  # GBT
            param_grid = ParamGridBuilder() \
                .addGrid(model.maxDepth, [5, 10]) \
                .addGrid(model.maxIter, [20, 50]) \
                .build()

        # Create cross-validator
        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=BinaryClassificationEvaluator(labelCol=label_col),
            numFolds=cv_folds
        )

        # Benchmark training time
        start_time = time.time()

        # Fit cross-validation model
        fitted_model = cv.fit(train_df)

        # Calculate training time
        training_time = time.time() - start_time

        best_model = fitted_model.bestModel
        trained_models[name] = best_model
        training_times[name] = training_time

        logger.info(f"{name} training completed in {training_time:.2f} seconds")

    return trained_models, training_times


def evaluate_models(trained_models, test_df, label_col):
    """
    Evaluate classification models using multiple metrics.

    Args:
        trained_models: Dictionary of trained models
        test_df: Test DataFrame
        label_col: Name of the label column

    Returns:
        list: List of tuples (model_name, metrics)
    """
    # Binary classification evaluator for AUC
    auc_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    # Multi-class evaluator for accuracy, precision, recall, F1
    multi_evaluator = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction"
    )

    results = []
    for name, model in trained_models.items():
        logger.info(f"Evaluating {name}...")

        # Benchmark evaluation time
        start_time = time.time()
        predictions = model.transform(test_df)
        eval_time = time.time() - start_time

        # Calculate metrics
        auc = auc_evaluator.evaluate(predictions)
        accuracy = multi_evaluator.setMetricName("accuracy").evaluate(predictions)
        precision = multi_evaluator.setMetricName("weightedPrecision").evaluate(predictions)
        recall = multi_evaluator.setMetricName("weightedRecall").evaluate(predictions)
        f1 = multi_evaluator.setMetricName("f1").evaluate(predictions)

        metrics = {
            "auc": auc,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "evaluation_time": eval_time
        }

        logger.info(f"{name} metrics:")
        for metric_name, value in metrics.items():
            if metric_name == "evaluation_time":
                logger.info(f"  {metric_name}: {value:.2f} seconds")
            else:
                logger.info(f"  {metric_name}: {value:.4f}")

        results.append((name, metrics))

    return results


def save_model(model, model_path):
    """
    Save a trained model to disk.

    Args:
        model: Trained model to save
        model_path: Path where model will be saved

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Saving model to {model_path}...")
        model.write().overwrite().save(model_path)
        logger.info(f"Model successfully saved to {model_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving model: {str(e)}")
        return False


def load_model(spark, model_path):
    """
    Load a previously saved pipeline model.

    Args:
        spark: SparkSession object
        model_path: Path to the saved model

    Returns:
        Loaded pipeline model or None if loading fails
    """
    try:
        logger.info(f"Loading model from {model_path}...")
        model = PipelineModel.load(model_path)
        logger.info("Model loaded successfully")
        return model
    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        return None