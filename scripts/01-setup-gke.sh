#!/bin/bash
set -e

# Configuración
PROJECT_ID="practica-creativa-494612"
CLUSTER_NAME="flight-prediction-gke"
ZONE="europe-west1-b"
REGION="europe-west1"
REGISTRY_REGION="us-central1"
REGISTRY_NAME="flight-prediction"

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Funciones
log_info() {
  echo -e "${GREEN}✅${NC} $1"
}

log_warn() {
  echo -e "${YELLOW}⚠️${NC} $1"
}

log_error() {
  echo -e "${RED}❌${NC} $1"
}

# Validar prerequisitos
echo "🔍 Validando prerequisitos..."

if ! command -v gcloud &> /dev/null; then
  log_error "gcloud no está instalado"
  exit 1
fi

if ! command -v kubectl &> /dev/null; then
  log_error "kubectl no está instalado"
  exit 1
fi

log_info "Prerequisitos validados"

# Configurar proyecto
echo ""
echo "📍 Configurando proyecto GCP..."
gcloud config set project $PROJECT_ID
log_info "Proyecto: $PROJECT_ID"

# Habilitar APIs
echo ""
echo "🔌 Habilitando APIs de GCP..."
gcloud services enable \
  container.googleapis.com \
  containerregistry.googleapis.com \
  artifactregistry.googleapis.com \
  compute.googleapis.com \
  servicenetworking.googleapis.com \
  --project=$PROJECT_ID

log_info "APIs habilitadas"

# Crear clúster GKE (si no existe)
echo ""
echo "☸️  Comprobando clúster GKE..."
CLUSTER_EXISTS=$(gcloud container clusters list \
  --zone $ZONE \
  --project $PROJECT_ID \
  --filter="name=$CLUSTER_NAME" \
  --format="value(name)" 2>/dev/null || echo "")

if [ -z "$CLUSTER_EXISTS" ]; then
  echo "🏗️  Creando clúster $CLUSTER_NAME (esto tarda ~5-10 min)..."
  gcloud container clusters create $CLUSTER_NAME \
    --zone $ZONE \
    --project $PROJECT_ID \
    --num-nodes 2 \
    --machine-type e2-standard-4 \
    --disk-type pd-standard \
    --disk-size 50 \
    --no-enable-autoupgrade \
    --release-channel None
  log_info "Clúster creado"
else
  log_warn "Clúster $CLUSTER_NAME ya existe"
fi

# Esperar a que el cluster esté RUNNING
echo ""
echo "⏳ Esperando a que el cluster esté RUNNING..."
max_attempts=60
attempt=0

while [ $attempt -lt $max_attempts ]; do
  STATUS=$(gcloud container clusters describe $CLUSTER_NAME \
    --zone $ZONE \
    --project $PROJECT_ID \
    --format="value(status)" 2>/dev/null || echo "PENDING")
  
  if [ "$STATUS" = "RUNNING" ]; then
    log_info "Cluster está RUNNING"
    break
  fi
  
  echo -ne "  Estado: $STATUS (intento $((attempt+1))/$max_attempts)\r"
  sleep 10
  attempt=$((attempt+1))
done

if [ "$STATUS" != "RUNNING" ]; then
  log_error "Cluster no llegó a RUNNING después de $((max_attempts*10)) segundos"
  exit 1
fi

# Configurar kubectl
echo ""
echo "🔐 Configurando kubectl..."
gcloud container clusters get-credentials $CLUSTER_NAME \
  --zone $ZONE \
  --project $PROJECT_ID

# Validar conexión
if kubectl cluster-info &> /dev/null; then
  log_info "kubectl configurado correctamente"
else
  log_error "No se puede conectar al cluster con kubectl"
  exit 1
fi

# Crear Artifact Registry
echo ""
echo "🏗️  Creando Artifact Registry..."
REGISTRY_EXISTS=$(gcloud artifacts repositories list \
  --location=$REGISTRY_REGION \
  --project=$PROJECT_ID \
  --filter="name:$REGISTRY_NAME" \
  --format="value(name)" 2>/dev/null || echo "")

if [ -z "$REGISTRY_EXISTS" ]; then
  gcloud artifacts repositories create $REGISTRY_NAME \
    --repository-format=docker \
    --location=$REGISTRY_REGION \
    --project=$PROJECT_ID \
    --description="Docker images for Flight Prediction"
  log_info "Artifact Registry creado"
else
  log_warn "Artifact Registry ya existe"
fi

# Configurar docker authentication para Artifact Registry
echo ""
echo "🐳 Configurando docker para Artifact Registry..."
gcloud auth configure-docker ${REGISTRY_REGION}-docker.pkg.dev --quiet
log_info "docker autenticado"

# Aplicar manifests de Fase 1
echo ""
echo "📦 Aplicando manifests Kubernetes..."

if [ -f "k8s/00-namespace/namespace.yaml" ]; then
  kubectl apply -f k8s/00-namespace/namespace.yaml
  log_info "Namespace aplicado"
else
  log_error "Archivo k8s/00-namespace/namespace.yaml no encontrado"
  exit 1
fi

if [ -f "k8s/01-rbac/serviceaccount.yaml" ]; then
  kubectl apply -f k8s/01-rbac/serviceaccount.yaml
  log_info "RBAC aplicado"
else
  log_error "Archivo k8s/01-rbac/serviceaccount.yaml no encontrado"
  exit 1
fi

if [ -f "k8s/02-configmaps/resourcequota.yaml" ]; then
  kubectl apply -f k8s/02-configmaps/resourcequota.yaml
  log_info "ResourceQuota aplicado"
else
  log_warn "Archivo k8s/02-configmaps/resourcequota.yaml no encontrado"
fi

if [ -f "k8s/02-configmaps/limitrange.yaml" ]; then
  kubectl apply -f k8s/02-configmaps/limitrange.yaml
  log_info "LimitRange aplicado"
else
  log_warn "Archivo k8s/02-configmaps/limitrange.yaml no encontrado"
fi

if [ -f "k8s/02-configmaps/configmap-endpoints.yaml" ]; then
  kubectl apply -f k8s/02-configmaps/configmap-endpoints.yaml
  log_info "ConfigMap de endpoints aplicado"
else
  log_warn "Archivo k8s/02-configmaps/configmap-endpoints.yaml no encontrado"
fi

if [ -f "k8s/02-configmaps/secrets-credentials.yaml" ]; then
  kubectl apply -f k8s/02-configmaps/secrets-credentials.yaml
  log_info "Secrets aplicados"
else
  log_warn "Archivo k8s/02-configmaps/secrets-credentials.yaml no encontrado"
fi

if [ -f "k8s/03-image-registry/storage.yaml" ]; then
  kubectl apply -f k8s/03-image-registry/storage.yaml
  log_info "StorageClasses aplicadas"
else
  log_warn "Archivo k8s/03-image-registry/storage.yaml no encontrado"
fi

# Resumen final
echo ""
echo "=================================="
echo "✅ FASE 1 COMPLETA"
echo "=================================="
echo ""
echo "📊 Status del cluster:"
kubectl get nodes
echo ""
echo "📊 Namespace y RBAC:"
kubectl get namespace flight-prediction
kubectl get sa -n flight-prediction
echo ""
echo "📊 Quotas y límites:"
kubectl describe resourcequota -n flight-prediction 2>/dev/null || echo "  (No aplicado aún)"
echo ""
echo "🎯 Próximos pasos:"
echo "1. Ejecutar: bash scripts/02-build-images.sh"
echo "2. Continuar con Fase 2: desplegar servicios stateful"
echo ""
echo "📖 Documentación:"
echo "  - Cluster: $CLUSTER_NAME en zona $ZONE"
echo "  - Artifact Registry: ${REGISTRY_REGION}-docker.pkg.dev/${PROJECT_ID}/${REGISTRY_NAME}"
echo "  - Namespace: flight-prediction"
echo ""