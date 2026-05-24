from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGE = "us-central1-docker.pkg.dev/practica-creativa-494612/flight-prediction/spark-predictor:latest"
NAMESPACE = "flight-prediction"
SERVICE_ACCOUNT = "flight-prediction-sa"

SPARK_SUBMIT_CMD = " ".join([
    "spark-submit",
    "--master k8s://https://kubernetes.default.svc:443",
    "--deploy-mode cluster",
    "--name train-flight-delay",
    f"--conf spark.kubernetes.container.image={IMAGE}",
    "--conf spark.kubernetes.container.image.pullPolicy=Always",
    f"--conf spark.kubernetes.namespace={NAMESPACE}",
    f"--conf spark.kubernetes.authenticate.driver.serviceAccountName={SERVICE_ACCOUNT}",
    "--conf spark.kubernetes.submission.waitAppCompletion=true",
    "--conf spark.scheduler.minRegisteredResourcesRatio=0",
    "--conf spark.scheduler.maxRegisteredResourcesWaitingTime=120s",
    "--conf spark.executor.instances=2",
    "--driver-memory 1g",
    "--executor-memory 1g",
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.local.type=hadoop",
    "--conf spark.sql.catalog.local.warehouse=s3a://lakehouse/warehouse",
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000",
    "--conf spark.hadoop.fs.s3a.access.key=minio",
    "--conf spark.hadoop.fs.s3a.secret.key=minio123",
    "--conf spark.hadoop.fs.s3a.path.style.access=true",
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "local:///app/train_spark_mllib_model.py .",
])

default_args = {
    "owner": "mvera",
    "retries": 0,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="train_flight_delay_model",
    description="Entrena el modelo de retraso de vuelos en Spark-on-K8s",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spark", "mllib", "flight-delay"],
) as dag:

    train = KubernetesPodOperator(
        task_id="spark_submit_train",
        name="spark-submit-train",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_policy="Always",
        service_account_name=SERVICE_ACCOUNT,
        cmds=["bash", "-lc"],
        arguments=[SPARK_SUBMIT_CMD],
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        log_events_on_failure=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "512Mi"},
            limits={"cpu": "500m", "memory": "1Gi"},
        ),
    )
