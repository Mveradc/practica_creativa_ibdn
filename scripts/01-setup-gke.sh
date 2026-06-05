#!/bin/bash
set -e

PROJECT_ID="practica-creativa-494612"
CLUSTER_NAME="flight-prediction-gke"
ZONE="europe-west1-b"
REGION="europe-west1"
REGISTRY_REGION="us-central1"
REGISTRY_NAME="flight-prediction"

# Validar prerequisitos
echo "Validando prerequisitos..."
command -v gcloud &>/dev/null || { echo "ERROR: gcloud no instalado"; exit 1; }
command -v kubectl &>/dev/null || { echo "ERROR: kubectl no instalado"; exit 1; }
echo "OK"

# Configurar proyecto
echo ""
echo "Configurando proyecto GCP..."
gcloud config set project $PROJECT_ID

# Habilitar APIs
echo ""
echo "Habilitando APIs de GCP..."
gcloud services enable \
  container.googleapis.com \
  containerregistry.googleapis.com \
  artifactregistry.googleapis.com \
  compute.googleapis.com \
  servicenetworking.googleapis.com \
  --project=$PROJECT_ID
echo "OK"

# Crear cluster GKE (si no existe)
echo ""
echo "Comprobando cluster GKE..."
CLUSTER_EXISTS=$(gcloud container clusters list \
  --zone $ZONE \
  --project $PROJECT_ID \
  --filter="name=$CLUSTER_NAME" \
  --format="value(name)" 2>/dev/null || echo "")

if [ -z "$CLUSTER_EXISTS" ]; then
  echo "Creando cluster $CLUSTER_NAME (esto tarda ~5-10 min)..."
  gcloud container clusters create $CLUSTER_NAME \
    --zone $ZONE \
    --project $PROJECT_ID \
    --num-nodes 2 \
    --machine-type e2-standard-4 \
    --disk-type pd-standard \
    --disk-size 50 \
    --no-enable-autoupgrade \
    --release-channel None
  echo "Cluster creado"
else
  echo "El cluster $CLUSTER_NAME ya existe"
fi

# Esperar a que el cluster este RUNNING
echo ""
echo "Esperando a que el cluster este RUNNING..."
max_attempts=60
attempt=0

while [ $attempt -lt $max_attempts ]; do
  STATUS=$(gcloud container clusters describe $CLUSTER_NAME \
    --zone $ZONE \
    --project $PROJECT_ID \
    --format="value(status)" 2>/dev/null || echo "PENDING")

  if [ "$STATUS" = "RUNNING" ]; then
    echo "Cluster RUNNING"
    break
  fi

  echo "  Estado: $STATUS (intento $((attempt+1))/$max_attempts)"
  sleep 10
  attempt=$((attempt+1))
done

if [ "$STATUS" != "RUNNING" ]; then
  echo "ERROR: Cluster no llego a RUNNING despues de $((max_attempts*10)) segundos"
  exit 1
fi

# Configurar kubectl
echo ""
echo "Configurando kubectl..."
gcloud container clusters get-credentials $CLUSTER_NAME \
  --zone $ZONE \
  --project $PROJECT_ID

kubectl cluster-info &>/dev/null || { echo "ERROR: No se puede conectar al cluster"; exit 1; }
echo "OK"

# Crear Artifact Registry
echo ""
echo "Creando Artifact Registry..."
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
  echo "Artifact Registry creado"
else
  echo "El Artifact Registry ya existe"
fi

gcloud auth configure-docker ${REGISTRY_REGION}-docker.pkg.dev --quiet
echo "Docker autenticado contra Artifact Registry"

# Resumen
echo ""
echo "== SETUP COMPLETADO =="
echo ""
echo "Cluster:          $CLUSTER_NAME ($ZONE)"
echo "Artifact Registry: ${REGISTRY_REGION}-docker.pkg.dev/${PROJECT_ID}/${REGISTRY_NAME}"
echo ""
kubectl get nodes
