#!/bin/bash
set -e

NAMESPACE="flight-prediction"
K8S_DIR="$(cd "$(dirname "$0")/../k8s" && pwd)"

command -v kubectl &>/dev/null || { echo "ERROR: kubectl no encontrado"; exit 1; }
kubectl cluster-info &>/dev/null || { echo "ERROR: sin conexion al cluster. Ejecuta: gcloud container clusters get-credentials <cluster> --region <region>"; exit 1; }

echo "Desplegando Flight Delay Prediction System"
echo "Directorio k8s: $K8S_DIR"
echo ""

# Fase 1: Infraestructura base
echo "=== Fase 1: Infraestructura base ==="
kubectl apply -f "$K8S_DIR/00-namespace/namespace.yaml"
kubectl apply -f "$K8S_DIR/01-rbac/serviceaccount.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/resourcequota.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/limitrange.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/configmap-endpoints.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/secrets-credentials.yaml"
kubectl apply -f "$K8S_DIR/03-image-registry/storage.yaml"

# Fase 2: Almacenamiento persistente
echo ""
echo "=== Fase 2: PVCs ==="
kubectl apply -f "$K8S_DIR/04-storage/pvcs.yaml"

# Fase 3: Kafka
echo ""
echo "=== Fase 3: Kafka ==="
kubectl apply -f "$K8S_DIR/05-kafka/kafka.yaml"
kubectl rollout status statefulset/kafka -n $NAMESPACE --timeout=120s
kubectl apply -f "$K8S_DIR/05-kafka/kafka-init-job.yaml"
kubectl wait --for=condition=complete job/kafka-init -n $NAMESPACE --timeout=120s

# Fase 4: Cassandra
echo ""
echo "=== Fase 4: Cassandra ==="
kubectl apply -f "$K8S_DIR/06-cassandra/cassandra.yaml"
kubectl rollout status statefulset/cassandra -n $NAMESPACE --timeout=180s
kubectl apply -f "$K8S_DIR/06-cassandra/cassandra-init-job.yaml"
kubectl wait --for=condition=complete job/cassandra-init -n $NAMESPACE --timeout=120s
bash resources/import_distances_cassandra_k8s.sh

# Fase 5: MinIO
echo ""
echo "=== Fase 5: MinIO ==="
kubectl apply -f "$K8S_DIR/07-minio/minio.yaml"
kubectl rollout status deployment/minio -n $NAMESPACE --timeout=120s
kubectl apply -f "$K8S_DIR/07-minio/minio-init-job.yaml"
kubectl wait --for=condition=complete job/minio-init -n $NAMESPACE --timeout=120s
bash resources/lakehouse/import_train_minio_k8s.sh

# Fase 6: Spark cluster (master + workers)
echo ""
echo "=== Fase 6: Spark cluster ==="
kubectl apply -f "$K8S_DIR/08-spark/spark-master.yaml"
kubectl rollout status deployment/spark-master -n $NAMESPACE --timeout=300s
kubectl apply -f "$K8S_DIR/08-spark/spark-workers.yaml"
kubectl rollout status deployment/spark-worker -n $NAMESPACE --timeout=300s
SPARK_MASTER_POD=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=spark-master -o jsonpath='{.items[0].metadata.name}')

# Fase 7: MLflow
echo ""
echo "=== Fase 7: MLflow ==="
kubectl apply -f "$K8S_DIR/09-mlflow/mlflow.yaml"
kubectl rollout status deployment/mlflow -n $NAMESPACE --timeout=300s

# Fase 8: Setup del lakehouse (tabla Iceberg)
echo ""
echo "=== Fase 8: Setup Iceberg ==="
kubectl exec -n $NAMESPACE "$SPARK_MASTER_POD" -- \
  bash -lc 'spark-submit \
    --master k8s://https://kubernetes.default.svc:443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=us-central1-docker.pkg.dev/practica-creativa-494612/flight-prediction/spark-predictor:latest \
    --conf spark.kubernetes.namespace=flight-prediction \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=flight-prediction-sa \
    --conf spark.scheduler.minRegisteredResourcesRatio=0 \
    --conf spark.scheduler.maxRegisteredResourcesWaitingTime=120s \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --executor-memory 512m \
    --driver-memory 512m \
    local:///app/setup_iceberg.py'

# Fase 9: Entrenamiento inicial del modelo
echo ""
echo "=== Fase 9: Entrenamiento del modelo ==="
kubectl exec -n $NAMESPACE "$SPARK_MASTER_POD" -- \
  bash -lc 'spark-submit \
    --master k8s://https://kubernetes.default.svc:443 \
    --deploy-mode cluster \
    --name train-flight-delay \
    --conf spark.kubernetes.container.image=us-central1-docker.pkg.dev/practica-creativa-494612/flight-prediction/spark-predictor:latest \
    --conf spark.kubernetes.namespace=flight-prediction \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=flight-prediction-sa \
    --conf spark.scheduler.minRegisteredResourcesRatio=0 \
    --conf spark.scheduler.maxRegisteredResourcesWaitingTime=120s \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.kubernetes.driverEnv.MLFLOW_S3_ENDPOINT_URL=http://minio:9000 \
    --conf spark.kubernetes.driverEnv.AWS_ACCESS_KEY_ID=minio \
    --conf spark.kubernetes.driverEnv.AWS_SECRET_ACCESS_KEY=minio123 \
    --executor-memory 1g \
    --driver-memory 1g \
    local:///app/train_spark_mllib_model.py .'

# Fase 10: Predictor en tiempo real (Spark Streaming)
echo ""
echo "=== Fase 10: Predictor en tiempo real ==="
kubectl apply -f "$K8S_DIR/08-spark/spark-submit.yaml"
kubectl rollout status deployment/spark-submit -n $NAMESPACE --timeout=300s

# Fase 11: PostgreSQL
echo ""
echo "=== Fase 11: PostgreSQL ==="
kubectl apply -f "$K8S_DIR/10-postgres/postgres.yaml"
kubectl rollout status statefulset/postgres -n $NAMESPACE --timeout=300s

# Fase 12: Airflow
echo ""
echo "=== Fase 12: Airflow ==="
kubectl apply -f "$K8S_DIR/11-airflow/airflow-dags-configmap.yaml"
kubectl apply -f "$K8S_DIR/11-airflow/airflow-init-job.yaml"
kubectl wait --for=condition=complete job/airflow-init -n $NAMESPACE --timeout=300s
kubectl apply -f "$K8S_DIR/11-airflow/airflow.yaml"
kubectl rollout status deployment/airflow-scheduler -n $NAMESPACE --timeout=300s
kubectl rollout status deployment/airflow-webserver -n $NAMESPACE --timeout=300s

# Fase 13: Flask API
echo ""
echo "=== Fase 13: Flask API ==="
kubectl apply -f "$K8S_DIR/12-flask/flask.yaml"
kubectl rollout status deployment/flask -n $NAMESPACE --timeout=300s

# Resumen
echo ""
echo "== DESPLIEGUE COMPLETO =="
echo ""
kubectl get all -n $NAMESPACE
echo ""
echo "IPs externas (puede tardar unos minutos en asignarse):"
kubectl get services -n $NAMESPACE --field-selector spec.type=LoadBalancer
echo ""
FLASK_IP=$(kubectl get svc flask -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
MLFLOW_IP=$(kubectl get svc mlflow-ui -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
AIRFLOW_IP=$(kubectl get svc airflow-webserver -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
SPARK_IP=$(kubectl get svc spark-master-ui -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
MINIO_IP=$(kubectl get svc minio-console -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
echo "Flask API:        http://${FLASK_IP}/flights/delays/predict_kafka"
echo "MLflow:           http://${MLFLOW_IP}:5050"
echo "Airflow:          http://${AIRFLOW_IP}:8082  (admin/admin)"
echo "Spark Master UI:  http://${SPARK_IP}:8080"
echo "MinIO Console:    http://${MINIO_IP}:9001    (minio/minio123)"
