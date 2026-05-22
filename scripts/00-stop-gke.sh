#!/bin/bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-practica-creativa-494612}"
CLUSTER_NAME="${CLUSTER_NAME:-flight-prediction-gke}"
ZONE="${ZONE:-europe-west1-b}"
ACTION="${1:-scale-down}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}✅${NC} $1"; }
log_warn() { echo -e "${YELLOW}⚠️${NC} $1"; }
log_error() { echo -e "${RED}❌${NC} $1"; }

usage() {
  cat <<EOF
Usage: $(basename "$0") [scale-down|delete]

scale-down: reduce node pools to 0 so the cluster stops consuming node compute.
delete: delete the cluster completely to stop all GKE charges.

Environment overrides:
  PROJECT_ID, CLUSTER_NAME, ZONE
EOF
}

if [[ "${ACTION}" == "-h" || "${ACTION}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v gcloud >/dev/null 2>&1; then
  log_error "gcloud no está instalado"
  exit 1
fi

echo "🛑 Preparando parada del cluster GKE"
echo "📍 Proyecto: ${PROJECT_ID}"
echo "📍 Cluster:  ${CLUSTER_NAME}"
echo "📍 Zona:     ${ZONE}"
echo "📍 Acción:   ${ACTION}"

gcloud config set project "${PROJECT_ID}" >/dev/null

if ! gcloud container clusters describe "${CLUSTER_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  log_warn "El cluster ${CLUSTER_NAME} no existe o no es accesible"
  exit 0
fi

if [[ "${ACTION}" == "delete" ]]; then
  echo "⚠️  Esto eliminará el cluster y detendrá todos los cobros de nodos y control plane asociados."
  read -r -p "Escribe DELETE para confirmar: " CONFIRMATION
  if [[ "${CONFIRMATION}" != "DELETE" ]]; then
    log_warn "Cancelado por el usuario"
    exit 1
  fi

  log_info "Eliminando cluster ${CLUSTER_NAME}..."
  gcloud container clusters delete "${CLUSTER_NAME}" \
    --zone "${ZONE}" \
    --project "${PROJECT_ID}" \
    --quiet

  log_info "Cluster eliminado"
  exit 0
fi

if [[ "${ACTION}" != "scale-down" ]]; then
  log_error "Acción no soportada: ${ACTION}"
  usage
  exit 1
fi

NODE_POOLS=$(gcloud container node-pools list \
  --cluster "${CLUSTER_NAME}" \
  --zone "${ZONE}" \
  --project "${PROJECT_ID}" \
  --format="value(name)")

if [[ -z "${NODE_POOLS}" ]]; then
  log_warn "No se encontraron node pools para escalar"
  exit 0
fi

echo "📉 Escalando node pools a 0..."
for NODE_POOL in ${NODE_POOLS}; do
  echo "  - ${NODE_POOL}"
  gcloud container clusters resize "${CLUSTER_NAME}" \
    --node-pool "${NODE_POOL}" \
    --num-nodes 0 \
    --zone "${ZONE}" \
    --project "${PROJECT_ID}" \
    --quiet
done

log_info "Node pools escalados a 0"
echo ""
echo "Nota: el control plane de GKE Standard puede seguir generando coste mínimo."
echo "Si quieres coste cero de GKE, ejecuta: $0 delete"
