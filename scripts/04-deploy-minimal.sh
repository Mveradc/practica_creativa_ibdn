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

echo "🚀 Desplegando setup mínimo: Kafka + Cassandra + Spark + Flask"
echo "📂 Directorio k8s: $K8S_DIR"
echo ""

if ! command -v kubectl &>/dev/null; then
  log_error "kubectl no encontrado"
  exit 1
fi

if ! kubectl cluster-info &>/dev/null; then
  log_error "No hay conexión con el cluster. Ejecuta: gcloud container clusters get-credentials <cluster> --region <region>"
  exit 1
fi

echo "=== Fase 1: Infraestructura base ==="
kubectl apply -f "$K8S_DIR/00-namespace/namespace.yaml"
kubectl apply -f "$K8S_DIR/01-rbac/serviceaccount.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/resourcequota.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/limitrange.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/configmap-endpoints.yaml"
kubectl apply -f "$K8S_DIR/02-configmaps/secrets-credentials.yaml"
kubectl apply -f "$K8S_DIR/03-image-registry/storage.yaml"
log_info "Infraestructura base aplicada"

echo ""
echo "=== Fase 2: Storage (solo modelos y Cassandra) ==="
kubectl apply -f "$K8S_DIR/04-storage/pvcs.yaml"
log_info "PVCs creados"

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
echo "=== Fase 5: Spark ==="
kubectl apply -f "$K8S_DIR/08-spark/spark-master.yaml"
kubectl rollout status deployment/spark-master -n $NAMESPACE --timeout=300s
kubectl apply -f "$K8S_DIR/08-spark/spark-workers.yaml"
kubectl rollout status deployment/spark-worker -n $NAMESPACE --timeout=300s
kubectl apply -f "$K8S_DIR/08-spark/spark-submit.yaml"
log_info "Spark cluster listo"

echo ""
echo "=== Fase 6: Flask API ==="
kubectl apply -f "$K8S_DIR/12-flask/flask.yaml"
kubectl rollout status deployment/flask -n $NAMESPACE --timeout=120s
log_info "Flask API lista"

echo ""
echo "=================================="
echo "✅ DESPLIEGUE MÍNIMO COMPLETO"
echo "=================================="
echo ""
kubectl get pods -n $NAMESPACE
echo ""
echo "🌐 URLs de acceso (cuando se asignen IPs externas):"
FLASK_IP=$(kubectl get svc flask -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
SPARK_IP=$(kubectl get svc spark-master-ui -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "<pendiente>")
echo "  Flask API:       http://${FLASK_IP}/flights/delays/predict_kafka"
echo "  Spark Master UI: http://${SPARK_IP}:8080"
