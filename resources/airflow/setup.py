import sys, os, re
import json
import logging
import urllib.request
import urllib.error

from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta, timezone
from airflow.operators.python import PythonOperator

PROJECT_HOME = os.getenv("PROJECT_HOME")


default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2016, 12, 1, tzinfo=timezone.utc),
  'retries': 3,
  'retry_delay': timedelta(minutes=5),
}

training_dag = DAG(
  'agile_data_science_batch_prediction_model_training',
  default_args=default_args,
  schedule_interval=None
)

# We use the same two commands for all our PySpark tasks
pyspark_bash_command = """
spark-submit --master {{ params.master }} \
  {{ params.base_path }}/{{ params.filename }} \
  {{ params.base_path }}
"""
pyspark_date_bash_command = """
spark-submit --master {{ params.master }} \
  {{ params.base_path }}/{{ params.filename }} \
  {{ ts }} {{ params.base_path }}
"""


# Gather the training data for our classifier
"""
extract_features_operator = BashOperator(
  task_id = "pyspark_extract_features",
  bash_command = pyspark_bash_command,
  params = {
    "master": "local[8]",
    "filename": "resources/extract_features.py",
    "base_path": "{}/".format(PROJECT_HOME)
  },
  dag=training_dag
)

"""

# Train and persist the classifier model
def submit_spark_via_rest(**context):
  submit_url = os.getenv("SPARK_REST_URL", "http://spark-master:6066/v1/submissions/create")
  master_url = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
  app_resource = "/app/train_spark_mllib_model.py"
  app_args = ["/app"]

  payload = {
    "action": "CreateSubmissionRequest",
    "appArgs": app_args,
    "appResource": app_resource,
    "clientSparkVersion": "3.4.0",
    "mainClass": "org.apache.spark.deploy.PythonRunner",
    "environmentVariables": {},
    "sparkProperties": {
      "spark.app.name": "train_spark_mllib_model",
      "spark.master": master_url,
      "spark.submit.deployMode": "cluster",
      "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2",
      "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.local.type": "hadoop",
      "spark.sql.catalog.local.warehouse": "s3a://lakehouse/warehouse",
      "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
      "spark.hadoop.fs.s3a.access.key": "minio",
      "spark.hadoop.fs.s3a.secret.key": "minio123",
    }
  }

  req = urllib.request.Request(submit_url, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
  try:
    with urllib.request.urlopen(req, timeout=30) as resp:
      resp_data = json.loads(resp.read().decode())
      logging.info("Spark submission response: %s", resp_data)
      if resp_data.get("success"):
        # push submission id to XCom for downstream inspection
        ti = context.get("ti")
        if ti and resp_data.get("submissionId"):
          ti.xcom_push(key="submission_id", value=resp_data.get("submissionId"))
      else:
        raise Exception("Spark submission failed: %s" % resp_data)
  except urllib.error.HTTPError as e:
    body = e.read().decode() if hasattr(e, 'read') else ''
    raise RuntimeError(f"Spark REST API HTTPError: {e.code} {e.reason} {body}")


train_classifier_model_operator = PythonOperator(
  task_id="pyspark_train_classifier_model",
  python_callable=submit_spark_via_rest,
  dag=training_dag,
)

# The model training depends on the feature extraction
#train_classifier_model_operator.set_upstream(extract_features_operator)
spark_submit_cmd = """
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://lakehouse/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minio \
  --conf spark.hadoop.fs.s3a.secret.key=minio123 \
  /app/train_spark_mllib_model.py /app
"""