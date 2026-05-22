#!/bin/bash
set -e

NAMESPACE="flight-prediction"
MINIO_SVC="minio"
MINIO_PORT="9000"
LOCAL_PORT="9000"
DATA_DIR="$(cd "$(dirname "$0")/../../data" && pwd)"
DATA_FILE="simple_flight_delay_features.jsonl.bz2"
MINIO_DEST="lakehouse/training-data/simple_flight_delay_features.jsonl.bz2"

# Validar fichero de datos
if [ ! -f "$DATA_DIR/$DATA_FILE" ]; then
  echo "Fichero no encontrado: $DATA_DIR/$DATA_FILE"
  exit 1
fi
echo "Fichero encontrado: $DATA_DIR/$DATA_FILE"

# Abrir port-forward en background
echo ""
echo "Abriendo port-forward a MinIO ($MINIO_SVC:$MINIO_PORT)..."
kubectl port-forward -n $NAMESPACE svc/$MINIO_SVC ${LOCAL_PORT}:${MINIO_PORT} &
PF_PID=$!

# Cerrar port-forward al salir (éxito o error)
trap "kill $PF_PID 2>/dev/null; echo '  Port-forward cerrado'" EXIT

# Esperar a que el tunnel esté listo
sleep 3

# Subir fichero con minio/mc en contenedor temporal
echo ""
echo "Subiendo datos a MinIO..."
docker run --rm --network host \
  -v "$DATA_DIR":/tmp/data \
  --entrypoint /bin/sh \
  minio/mc \
  -c "
    mc alias set local http://localhost:${LOCAL_PORT} minio minio123 --quiet;
    mc mb --ignore-existing local/lakehouse;
    mc mb --ignore-existing local/lakehouse/training-data;
    mc cp /tmp/data/${DATA_FILE} local/${MINIO_DEST};
    echo 'Verificando subida...';
    mc ls local/lakehouse/training-data/;
  "

echo "Datos subidos a s3a://$MINIO_DEST"
echo ""
echo "Próximo paso: ejecutar el job de entrenamiento"
echo "   bash scripts/06-train-model.sh"
