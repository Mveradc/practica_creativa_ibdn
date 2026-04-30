FROM eclipse-temurin:17-jdk-jammy

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copiar Spark desde la VM al contexto de build
COPY spark/ /opt/spark/

WORKDIR /app
COPY target/scala-2.13/flight_prediction_2.13-0.1.jar /app/flight_prediction.jar


# --- Directorio donde se montarán los modelos como volumen ---
RUN mkdir -p /app/models
ENV SPARK_PACKAGES="org.mongodb.spark:mongo-spark-connector_2.13:10.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

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