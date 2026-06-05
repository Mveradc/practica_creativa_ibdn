# !/usr/bin/env python

import sys, os, re
from os import environ

# Pass date and base path to main() from airflow
def main(base_path, use_lakehouse=True, use_mlflow=True):
  
  # Default to "."
  try: base_path
  except NameError: base_path = "."
  if not base_path:
    base_path = "."
  
  APP_NAME = "train_spark_mllib_model.py"
  
  # If there is no SparkSession, create the environment
  from pyspark.sql import SparkSession

  spark_builder = SparkSession.builder.appName(APP_NAME)

  if use_lakehouse:
    spark_builder = spark_builder \
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
      .config("spark.sql.catalog.local.type", "hadoop") \
      .config("spark.sql.catalog.local.warehouse", "s3a://lakehouse/warehouse") \
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
      .config("spark.hadoop.fs.s3a.access.key", "minio") \
      .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
      .config("spark.hadoop.fs.s3a.path.style.access", "true") \
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
      .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
      .config("spark.hadoop.fs.s3a.socket.timeout", "60000")

  spark = spark_builder.getOrCreate()

  if use_mlflow:
    try:
      import mlflow
      mlflow.set_tracking_uri("http://mlflow:5050")
      mlflow.set_experiment("flight_delay_prediction")
      mlflow.start_run()
    except Exception as e:
      print(f"MLflow not available: {e}. Continuing without tracking.")
  
  #
  # {
  #   "ArrDelay":5.0,"CRSArrTime":"2015-12-31T03:20:00.000-08:00","CRSDepTime":"2015-12-31T03:05:00.000-08:00",
  #   "Carrier":"WN","DayOfMonth":31,"DayOfWeek":4,"DayOfYear":365,"DepDelay":14.0,"Dest":"SAN","Distance":368.0,
  #   "FlightDate":"2015-12-30T16:00:00.000-08:00","FlightNum":"6109","Origin":"TUS"
  # }
  #
  from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
  from pyspark.sql.types import StructType, StructField
  from pyspark.sql.functions import udf
  
  schema = StructType([
    StructField("ArrDelay", DoubleType(), True),     # "ArrDelay":5.0
    StructField("CRSArrTime", TimestampType(), True),    # "CRSArrTime":"2015-12-31T03:20:00.000-08:00"
    StructField("CRSDepTime", TimestampType(), True),    # "CRSDepTime":"2015-12-31T03:05:00.000-08:00"
    StructField("Carrier", StringType(), True),     # "Carrier":"WN"
    StructField("DayOfMonth", IntegerType(), True), # "DayOfMonth":31
    StructField("DayOfWeek", IntegerType(), True),  # "DayOfWeek":4
    StructField("DayOfYear", IntegerType(), True),  # "DayOfYear":365
    StructField("DepDelay", DoubleType(), True),     # "DepDelay":14.0
    StructField("Dest", StringType(), True),        # "Dest":"SAN"
    StructField("Distance", DoubleType(), True),     # "Distance":368.0
    StructField("FlightDate", DateType(), True),    # "FlightDate":"2015-12-30T16:00:00.000-08:00"
    StructField("FlightNum", StringType(), True),   # "FlightNum":"6109"
    StructField("Origin", StringType(), True),      # "Origin":"TUS"
  ])
  
  # Load data: from lakehouse (Iceberg) or local storage
  if use_lakehouse:
    print("Loading data from Iceberg lakehouse: local.db.vuelos")
    features = spark.read.table("local.db.vuelos")
  else:
    print("Loading data from local storage")
    input_path = "{}/data/simple_flight_delay_features.jsonl.bz2".format(base_path)
    features = spark.read.json(input_path, schema=schema)
  features.first()
  
  #
  # Check for nulls in features before using Spark ML
  #
  null_counts = [(column, features.where(features[column].isNull()).count()) for column in features.columns]
  cols_with_nulls = filter(lambda x: x[1] > 0, null_counts)
  print(list(cols_with_nulls))
  
  #
  # Add a Route variable to replace FlightNum
  #
  from pyspark.sql.functions import lit, concat
  features_with_route = features.withColumn(
    'Route',
    concat(
      features.Origin,
      lit('-'),
      features.Dest
    )
  )
  features_with_route.show(6)
  
  #
  # Use pysmark.ml.feature.Bucketizer to bucketize ArrDelay into on-time, slightly late, very late (0, 1, 2)
  #
  from pyspark.ml.feature import Bucketizer
  
  # Setup the Bucketizer
  splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
  arrival_bucketizer = Bucketizer(
    splits=splits,
    inputCol="ArrDelay",
    outputCol="ArrDelayBucket"
  )
  
  # Save the bucketizer (to MinIO if using lakehouse, else local)
  if use_lakehouse:
    arrival_bucketizer_path = "s3a://lakehouse/models/arrival_bucketizer_2.0.bin"
  else:
    arrival_bucketizer_path = "{}/models/arrival_bucketizer_2.0.bin".format(base_path)
  arrival_bucketizer.write().overwrite().save(arrival_bucketizer_path)
  
  # Apply the bucketizer
  ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
  ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()
  
  #
  # Extract features tools in with pyspark.ml.feature
  #
  from pyspark.ml.feature import StringIndexer, VectorAssembler
  
  # Turn category fields into indexes
  for column in ["Carrier", "Origin", "Dest", "Route"]:
    string_indexer = StringIndexer(
      inputCol=column,
      outputCol=column + "_index"
    )
    
    string_indexer_model = string_indexer.fit(ml_bucketized_features)
    ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
    
    # Drop the original column
    ml_bucketized_features = ml_bucketized_features.drop(column)
    
    # Save the pipeline model
    if use_lakehouse:
      string_indexer_output_path = "s3a://lakehouse/models/string_indexer_model_{}.bin".format(column)
    else:
      string_indexer_output_path = "{}/models/string_indexer_model_{}.bin".format(
        base_path,
        column
      )
    string_indexer_model.write().overwrite().save(string_indexer_output_path)
  
  # Combine continuous, numeric fields with indexes of nominal ones
  # ...into one feature vector
  numeric_columns = [
    "DepDelay", "Distance",
    "DayOfMonth", "DayOfWeek",
    "DayOfYear"]
  index_columns = ["Carrier_index", "Origin_index",
                   "Dest_index", "Route_index"]
  vector_assembler = VectorAssembler(
    inputCols=numeric_columns + index_columns,
    outputCol="Features_vec"
  )
  final_vectorized_features = vector_assembler.transform(ml_bucketized_features)
  
  # Save the numeric vector assembler
  if use_lakehouse:
    vector_assembler_path = "s3a://lakehouse/models/numeric_vector_assembler.bin"
  else:
    vector_assembler_path = "{}/models/numeric_vector_assembler.bin".format(base_path)
  vector_assembler.write().overwrite().save(vector_assembler_path)
  
  # Drop the index columns
  for column in index_columns:
    final_vectorized_features = final_vectorized_features.drop(column)
  
  # Inspect the finalized features
  final_vectorized_features.show()
  
  # Instantiate and fit random forest classifier on all the data
  from pyspark.ml.classification import RandomForestClassifier
  rfc = RandomForestClassifier(
    featuresCol="Features_vec",
    labelCol="ArrDelayBucket",
    predictionCol="Prediction",
    maxBins=4657,
    maxMemoryInMB=1024
  )
  model = rfc.fit(final_vectorized_features)
  
  # Save the new model over the old one
  if use_lakehouse:
    model_output_path = "s3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin"
  else:
    model_output_path = "{}/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(
      base_path
    )
  model.write().overwrite().save(model_output_path)
  
  # Evaluate model using test data
  predictions = model.transform(final_vectorized_features)
  
  from pyspark.ml.evaluation import MulticlassClassificationEvaluator
  evaluator = MulticlassClassificationEvaluator(
    predictionCol="Prediction",
    labelCol="ArrDelayBucket",
    metricName="accuracy"
  )
  accuracy = evaluator.evaluate(predictions)
  print("Accuracy = {}".format(accuracy))
  
  # Log metrics and model to MLflow if enabled
  if use_mlflow:
    try:
      import mlflow
      import mlflow.spark
      mlflow.log_metric("accuracy", accuracy)
      mlflow.log_param("use_lakehouse", use_lakehouse)
      mlflow.log_param("num_trees", 20)  # RandomForestClassifier default
      mlflow.log_param("max_bins", 4657)
      mlflow.spark.log_model(model, "spark-model", registered_model_name="flight_delay_classifier")
    except Exception as e:
      print(f"Could not log to MLflow: {e}")
  
  # Check the distribution of predictions
  predictions.groupBy("Prediction").count().show()
  
  # Check a sample
  predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)
  
  # End MLflow run if active
  if use_mlflow:
    try:
      import mlflow
      mlflow.end_run()
    except Exception as e:
      print(f"Could not end MLflow run: {e}")

if __name__ == "__main__":
  # Usage: python train_spark_mllib_model.py [base_path] [--local] [--no-mlflow]
  base_path = sys.argv[1] if len(sys.argv) > 1 else "."
  use_lakehouse = "--local" not in sys.argv
  use_mlflow = "--no-mlflow" not in sys.argv
  
  main(base_path, use_lakehouse=use_lakehouse, use_mlflow=use_mlflow)


"""

docker exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minio \
  --conf spark.hadoop.fs.s3a.secret.key=minio123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.minio.type=hadoop \
  --conf "spark.sql.catalog.minio.warehouse=s3a://lakehouse/warehouse" \
  /app/train_spark_mllib_model.py .


kubectl exec -n flight-prediction spark-master-7fdcdd7468-vmw6w -- \
  bash -lc 'spark-submit \
    --master k8s://https://kubernetes.default.svc:443 \
    --deploy-mode cluster \
    --name train-flight-delay \
    --conf spark.kubernetes.container.image=us-central1-docker.pkg.dev/practica-creativa-494612/flight-prediction/spark-predictor:latest \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.namespace=flight-prediction \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=flight-prediction-sa \
    --conf spark.kubernetes.driver.podTemplateContainerName=spark-kubernetes-driver \
    --conf spark.scheduler.minRegisteredResourcesRatio=0 \
    --conf spark.scheduler.maxRegisteredResourcesWaitingTime=120s \
    --conf spark.executor.instances=2 \
    --driver-memory 1g \
    --executor-memory 1g \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=s3a://lakehouse/warehouse \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    local:///app/train_spark_mllib_model.py .'
"""