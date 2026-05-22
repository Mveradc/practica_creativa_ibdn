#!/bin/bash
set -eo pipefail

# Configuración
PROJECT_ID="practica-creativa-494612"
REGISTRY_REGION="us-central1"
REGISTRY_NAME="flight-prediction"
REGISTRY_URL="${REGISTRY_REGION}-docker.pkg.dev/${PROJECT_ID}/${REGISTRY_NAME}"

# Colores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
  echo -e "${GREEN}✅${NC} $1"
}

log_warn() {
  echo -e "${YELLOW}⚠️${NC} $1"
}

log_error() {
  echo -e "${RED}❌${NC} $1"
}

# Obtener tag
if command -v git &> /dev/null && [ -d .git ]; then
  TAG=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
else
  TAG=$(date +%s)
fi

echo "🐳 Build y push de imágenes"
echo "📍 Registry: $REGISTRY_URL"
echo "🏷️  Tag: $TAG"
echo ""

# Verificar que docker está disponible
if ! command -v docker &> /dev/null; then
  log_error "Docker no está instalado"
  exit 1
fi

# Verificar que el registry está accesible
echo "🔐 Validando acceso a Artifact Registry..."
if docker pull ${REGISTRY_URL}/spark-predictor:latest 2>/dev/null; then
  log_warn "Imagen anterior encontrada (OK)"
elif docker pull busybox:latest 2>&1 | grep -q "denied"; then
  log_error "Error de autenticación con Artifact Registry"
  echo "  Ejecuta: gcloud auth configure-docker ${REGISTRY_REGION}-docker.pkg.dev"
  exit 1
else
  log_info "Acceso validado"
fi

# Spark image
echo ""
echo "=== Building Spark Image ==="
if [ ! -f "flight_prediction/Dockerfile" ]; then
  log_error "flight_prediction/Dockerfile no encontrado"
  exit 1
fi

echo "🔨 Building..."
docker build \
  -t ${REGISTRY_URL}/spark-predictor:${TAG} \
  -t ${REGISTRY_URL}/spark-predictor:latest \
  -f flight_prediction/Dockerfile \
  . 2>&1 | tail -5

echo "📤 Pushing..."
docker push ${REGISTRY_URL}/spark-predictor:${TAG}
docker push ${REGISTRY_URL}/spark-predictor:latest

log_info "Spark image: ${REGISTRY_URL}/spark-predictor:${TAG}"

# Flask image
echo ""
echo "=== Building Flask Image ==="
if [ ! -f "resources/web/Dockerfile" ]; then
  log_error "resources/web/Dockerfile no encontrado"
  exit 1
fi

echo "🔨 Building..."
docker build \
  -t ${REGISTRY_URL}/flask-api:${TAG} \
  -t ${REGISTRY_URL}/flask-api:latest \
  -f resources/web/Dockerfile \
  resources/web/ 2>&1 | tail -5

echo "📤 Pushing..."
docker push ${REGISTRY_URL}/flask-api:${TAG}
docker push ${REGISTRY_URL}/flask-api:latest

log_info "Flask image: ${REGISTRY_URL}/flask-api:${TAG}"

# Listado final
echo ""
echo "=== Imágenes en Artifact Registry ==="
gcloud artifacts docker images list ${REGISTRY_URL} --limit=10 --project=$PROJECT_ID

echo ""
echo "=================================="
echo "✅ BUILD COMPLETE"
echo "=================================="
echo ""
echo "📋 Imágenes disponibles:"
echo "  Spark: ${REGISTRY_URL}/spark-predictor:${TAG}"
echo "  Flask: ${REGISTRY_URL}/flask-api:${TAG}"
echo ""
echo "🎯 Próximo paso: Fase 2 (datos y stateful services)"
echo ""