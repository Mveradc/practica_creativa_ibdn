#!/bin/bash
set -eo pipefail

PROJECT_ID="practica-creativa-494612"
REGISTRY_REGION="us-central1"
REGISTRY_NAME="flight-prediction"
REGISTRY_URL="${REGISTRY_REGION}-docker.pkg.dev/${PROJECT_ID}/${REGISTRY_NAME}"

command -v docker &>/dev/null || { echo "ERROR: docker no instalado"; exit 1; }

if command -v git &>/dev/null && [ -d .git ]; then
  TAG=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
else
  TAG=$(date +%s)
fi

echo "Build y push de imagenes"
echo "Registry: $REGISTRY_URL"
echo "Tag:      $TAG"
echo ""

# Spark image
echo "=== Spark image ==="
[ -f "flight_prediction/Dockerfile" ] || { echo "ERROR: flight_prediction/Dockerfile no encontrado"; exit 1; }
docker build \
  -t ${REGISTRY_URL}/spark-predictor:${TAG} \
  -t ${REGISTRY_URL}/spark-predictor:latest \
  -f flight_prediction/Dockerfile \
  . 2>&1 | tail -5
docker push ${REGISTRY_URL}/spark-predictor:${TAG}
docker push ${REGISTRY_URL}/spark-predictor:latest
echo "OK: ${REGISTRY_URL}/spark-predictor:${TAG}"

# Flask image
echo ""
echo "=== Flask image ==="
[ -f "resources/web/Dockerfile" ] || { echo "ERROR: resources/web/Dockerfile no encontrado"; exit 1; }
docker build \
  -t ${REGISTRY_URL}/flask-api:${TAG} \
  -t ${REGISTRY_URL}/flask-api:latest \
  -f resources/web/Dockerfile \
  resources/web/ 2>&1 | tail -5
docker push ${REGISTRY_URL}/flask-api:${TAG}
docker push ${REGISTRY_URL}/flask-api:latest
echo "OK: ${REGISTRY_URL}/flask-api:${TAG}"

echo ""
echo "== BUILD COMPLETO =="
echo ""
gcloud artifacts docker images list ${REGISTRY_URL} --limit=10 --project=$PROJECT_ID
echo ""
echo "Siguiente paso: bash scripts/03-deploy.sh"
