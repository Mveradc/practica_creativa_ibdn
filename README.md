# Despliegue en Kubernetes (GKE)

Guía para desplegar el sistema de predicción de retrasos de vuelos en Google
Kubernetes Engine (GKE).

## Componentes que se despliegan

- Kafka (KRaft) — cola de mensajes
- Cassandra — distancias entre aeropuertos
- MinIO — almacenamiento S3 para el lakehouse (Iceberg)
- Spark (master + workers) — entrenamiento y predicción en streaming
- MLflow — tracking de modelos
- PostgreSQL — metadatos de Airflow
- Airflow — orquestación del DAG
- Flask — frontend web de predicción

## 1. Prerrequisitos

Necesitas una cuenta de GCP con facturación activa y las siguientes
herramientas instaladas en tu máquina.

### gcloud (Google Cloud CLI)

```bash
# Debian/Ubuntu
sudo apt-get update && sudo apt-get install -y apt-transport-https ca-certificates gnupg curl
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update && sudo apt-get install -y google-cloud-cli google-cloud-cli-gke-gcloud-auth-plugin
```

### kubectl

```bash
gcloud components install kubectl
# o bien
sudo apt-get install -y kubectl
```

### docker

```bash
sudo apt-get install -y docker.io
sudo usermod -aG docker $USER   # reiniciar sesion despues
```

### Autenticación en GCP

```bash
gcloud auth login
gcloud config set project practica-creativa-494612
```

> Nota: el `PROJECT_ID` (`practica-creativa-494612`) y la zona
> (`europe-west1-b`) están fijados en los scripts. Si usas otro proyecto,
> edita las variables al inicio de `scripts/01-setup-gke.sh`,
> `scripts/02-build-images.sh` y los `--conf spark.kubernetes.container.image`
> de `scripts/03-deploy.sh`.

## 2. Crear el cluster y el registro de imágenes

Crea el cluster GKE (2 nodos `e2-standard-4`), habilita las APIs necesarias,
configura `kubectl` y crea el Artifact Registry para las imágenes Docker.

```bash
bash scripts/01-setup-gke.sh
```

Tarda ~5-10 minutos en crear el cluster.

## 3. Construir y subir las imágenes

Construye las imágenes de Spark (predictor/entrenamiento) y de Flask, y las
sube al Artifact Registry.

```bash
bash scripts/02-build-images.sh
```

## 4. Desplegar todo

Despliega todos los componentes en orden, espera a que cada uno esté listo,
importa las distancias en Cassandra, carga los datos de entrenamiento en MinIO,
crea la tabla Iceberg, entrena el modelo y arranca el predictor en streaming.

```bash
bash scripts/03-deploy.sh
```

Al final imprime las IPs externas (LoadBalancer) de cada servicio. Pueden
tardar un par de minutos en asignarse.

## 5. Acceder a los servicios

```bash
kubectl get services -n flight-prediction --field-selector spec.type=LoadBalancer
```

| Servicio        | URL                                                  | Credenciales     |
|-----------------|------------------------------------------------------|------------------|
| Flask (web)     | `http://<FLASK_IP>/flights/delays/predict_kafka`     | -                |
| MLflow          | `http://<MLFLOW_IP>:5050`                             | -                |
| Airflow         | `http://<AIRFLOW_IP>:8082`                            | admin / admin    |
| Spark Master UI | `http://<SPARK_IP>:8080`                             | -                |
| MinIO Console   | `http://<MINIO_IP>:9001`                             | minio / minio123 |

## 6. Probar la predicción

1. Abre `http://<FLASK_IP>/flights/delays/predict_kafka`.
2. Rellena el formulario y envía la predicción.
3. El frontend envía la petición a Kafka; Spark Streaming la procesa y guarda
   el resultado, que aparece en la página tras unos segundos.

## Comandos útiles

```bash
# Estado de todos los recursos
kubectl get all -n flight-prediction

# Logs de un componente
kubectl logs -n flight-prediction deployment/flask
kubectl logs -n flight-prediction deployment/spark-submit

# Pods con problemas
kubectl get pods -n flight-prediction

# Reconectar kubectl al cluster (si pierdes credenciales)
gcloud container clusters get-credentials flight-prediction-gke --zone europe-west1-b
```

## Limpieza

Para no incurrir en costes, elimina el cluster y el registro cuando termines:

```bash
gcloud container clusters delete flight-prediction-gke --zone europe-west1-b
gcloud artifacts repositories delete flight-prediction --location us-central1
```
