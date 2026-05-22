#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}✅${NC} $1"; }
log_warn()  { echo -e "${YELLOW}⚠️${NC} $1"; }
log_error() { echo -e "${RED}❌${NC} $1"; }

NAMESPACE="flight-prediction"
K8S_DIR="$(cd "$(dirname "$0")/../k8s" && pwd)"

echo "🚀 Desplegando Flight Delay Prediction System en Kubernetes"
echo "📂 Directorio k8s: $K8S_DIR"
echo ""

# Verificar prerequisitos
if ! command -v kubectl &>/dev/null; then
  log_error "kubectl no encontrado"
  exit 1
fi

if ! kubectl cluster-info &>/dev/null; then
  log_error "No hay conexión con el cluster. Ejecuta: gcloud container clusters get-credentials <cluster> --region <region>"
  exit 1
fi

# Fase 1: Infraestructura base
echo "=== Fase 1: Infraestructura base ==="
kubectl apply -f "$K8S_DIR/00-namespace/namespace.yaml"
kubectl apply -f "$K8S_DIR/01-rbac/serviceaccount.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/resourcequota.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/limitrange.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/configmap-endpoints.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/secrets-credentials.yaml"
kubectl apply -f "$K8S_DIR/03-image-registry/storage.yaml"
log_info "Infraestructura base aplicada"

# Fase 2: Almacenamiento persistente
echo ""
echo "=== Fase 2: PVCs ==="
kubectl apply -f "$K8S_DIR/04-storage/pvcs.yaml"
log_info "PVCs creados"

# Fase 3: Servicios de datos (stateful)
echo ""
echo "=== Fase 3: Kafka ==="
kubectl apply -f "$K8S_DIR/05-kafka/kafka.yaml"
echo "Esperando a que Kafka esté listo..."
kubectl rollout status statefulset/kafka -n $NAMESPACE --timeout=120s
kubectl apply -f "$K8S_DIR/05-kafka/kafka-init-job.yaml"
kubectl wait --for=condition=complete job/kafka-init -n $NAMESPACE --timeout=120s
log_info "Kafka listo y topics creados"

echo ""
echo "=== Fase 4: Cassandra ==="
kubectl apply -f "$K8S_DIR/06-cassandra/cassandra.yaml"
echo "Esperando a que Cassandra esté listo (puede tardar ~2 min)..."
kubectl rollout status statefulset/cassandra -n $NAMESPACE --timeout=180s
kubectl apply -f "$K8S_DIR/06-cassandra/cassandra-init-job.yaml"
kubectl wait --for=condition=complete job/cassandra-init -n $NAMESPACE --timeout=120s
log_info "Cassandra listo y schema inicializado"

echo ""
echo "=== Fase 5: MinIO ==="
kubectl apply -f "$K8S_DIR/07-minio/minio.yaml"
kubectl rollout status deployment/minio -n $NAMESPACE --timeout=120s
kubectl apply -f "$K8S_DIR/07-minio/minio-init-job.yaml"
kubectl wait --for=condition=complete job/minio-init -n $NAMESPACE --timeout=120s
log_info "MinIO listo y buckets creados"

# Fase 4: Capa de cómputo
echo ""
echo "=== Fase 6: Spark ==="
kubectl apply -f "$K8S_DIR/08-spark/spark-master.yaml"
kubectl rollout status deployment/spark-master -n $NAMESPACE --timeout=300s
kubectl apply -f "$K8S_DIR/08-spark/spark-workers.yaml"
kubectl rollout status deployment/spark-worker -n $NAMESPACE --timeout=300s
kubectl apply -f "$K8S_DIR/08-spark/spark-submit.yaml"
log_info "Spark cluster listo"

# Fase 5: MLflow
echo ""
echo "=== Fase 7: MLflow ==="
kubectl apply -f "$K8S_DIR/09-mlflow/mlflow.yaml"
kubectl rollout status deployment/mlflow -n $NAMESPACE --timeout=120s
log_info "MLflow listo"

# Fase 6: PostgreSQL + Airflow
echo ""
echo "=== Fase 8: PostgreSQL ==="
kubectl apply -f "$K8S_DIR/10-postgres/postgres.yaml"
kubectl rollout status statefulset/postgres -n $NAMESPACE --timeout=120s
log_info "PostgreSQL listo"

echo ""
echo "=== Fase 9: Airflow ==="
kubectl apply -f "$K8S_DIR/11-airflow/airflow-init-job.yaml"
kubectl wait --for=condition=complete job/airflow-init -n $NAMESPACE --timeout=180s
kubectl apply -f "$K8S_DIR/11-airflow/airflow.yaml"
kubectl rollout status deployment/airflow-scheduler -n $NAMESPACE --timeout=120s
kubectl rollout status deployment/airflow-webserver -n $NAMESPACE --timeout=120s
log_info "Airflow listo (admin/admin)"

# Fase 7: Flask API
echo ""
echo "=== Fase 10: Flask API ==="
kubectl apply -f "$K8S_DIR/12-flask/flask.yaml"
kubectl rollout status deployment/flask -n $NAMESPACE --timeout=120s
log_info "Flask API listo"

# Resumen
echo ""
echo "=================================="
echo "✅ DESPLIEGUE COMPLETO"
echo "=================================="
echo ""
echo "📋 Estado del namespace:"
kubectl get all -n $NAMESPACE
echo ""
echo "🌐 IPs externas (puede tardar unos minutos en asignarse):"
kubectl get services -n $NAMESPACE --field-selector spec.type=LoadBalancer
echo ""
echo "🔗 URLs de acceso (cuando se asignen IPs):"
FLASK_IP=$(kubectl get svc flask -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
MLFLOW_IP=$(kubectl get svc mlflow-ui -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
AIRFLOW_IP=$(kubectl get svc airflow-webserver -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
SPARK_IP=$(kubectl get svc spark-master-ui -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
MINIO_IP=$(kubectl get svc minio-console -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
echo "  Flask API:       http://${FLASK_IP}/flights/delays/predict_kafka"
echo "  MLflow:          http://${MLFLOW_IP}:5050"
echo "  Airflow:         http://${AIRFLOW_IP}:8082  (admin/admin)"
echo "  Spark Master UI: http://${SPARK_IP}:8080"
echo "  MinIO Console:   http://${MINIO_IP}:9001   (minio/minio123)"
echo ""
