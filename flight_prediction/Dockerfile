FROM eclipse-temurin:17-jdk-jammy

# --- Instalar Spark 4.1.1 ---
ENV SPARK_VERSION=4.1.1
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY spark/ /opt/spark/

# --- Directorio de trabajo ---
WORKDIR /app

# --- Copiar el JAR compilado ---
# Requiere haber ejecutado 'sbt package' antes de hacer docker build
COPY target/scala-2.13/flight_prediction_2.13-0.1.jar /app/flight_prediction.jar

# --- Directorio donde se montarán los modelos como volumen ---
RUN mkdir -p /app/models

# --- Pre-descargar dependencias de Spark para no hacerlo en cada arranque ---
# Esto descarga kafka y mongo connector al ivy cache durante el build
ENV SPARK_PACKAGES="org.mongodb.spark:mongo-spark-connector_2.13:10.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

RUN spark-submit \
    --packages ${SPARK_PACKAGES} \
    --class es.upm.dit.ging.predictor.MakePrediction \
    --dry-run \
    /app/flight_prediction.jar || true

# --- Variables de entorno configurables por docker-compose ---
ENV KAFKA_BROKERS=localhost:9092
ENV MONGO_URI=mongodb://localhost:27017
ENV BASE_PATH=/app

# --- Comando de arranque ---
CMD spark-submit \
    --packages ${SPARK_PACKAGES} \
    --master local[*] \
    --class es.upm.dit.ging.predictor.MakePrediction \
    --conf spark.driver.extraJavaOptions="-Dlog4j.rootCategory=WARN,console" \
    /app/flight_prediction.jar
