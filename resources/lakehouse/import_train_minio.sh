#!/bin/bash

ls ./data

# Ejecutamos el contenedor con una pequeña comprobación extra
docker run --rm --network host \
  -v $(pwd)/data:/tmp/data \
  --entrypoint /bin/sh \
  minio/mc \
  -c "
    mc alias set local http://localhost:9000 minio minio123;
    mc mb --ignore-existing local/lakehouse;
    
    echo 'Buscando archivo dentro del contenedor...';
    ls /tmp/data;
    
    mc cp /tmp/data/simple_flight_delay_features.jsonl.bz2 local/lakehouse/training-data/;
  "