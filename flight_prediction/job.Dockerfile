FROM spark:4.1.1-scala2.13-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_PACKAGES="com.datastax.oss:java-driver-core:4.17.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:$PATH

# Directorio de trabajo
WORKDIR /app

# Copiar requirements.txt ANTES de instalar dependencias
COPY requirements.txt /app/requirements.txt

# Instalar dependencias Python
RUN pip3 install --upgrade pip setuptools wheel && \
    pip3 install --no-cache-dir -r /app/requirements.txt

# Copiar el JAR compilado
COPY target/scala-2.13/flight_prediction_2.13-0.1.jar /app/flight_prediction.jar

# Directorio donde se montarán los modelos como volumen
RUN mkdir -p /app/models
RUN chown -R spark:spark /app/models
RUN mkdir -p /nonexistent/.ivy2.5.2/cache /nonexistent/.ivy2.5.2/jars && chown -R spark:spark /nonexistent

# Variables de entorno configurables por docker-compose
ENV KAFKA_BROKERS=kafka:9092
ENV CASSANDRA_HOST=cassandra
ENV CASSANDRA_PORT=9042
ENV CASSANDRA_DATACENTER=datacenter1
ENV BASE_PATH=/app

USER spark

# Comando de arranque
CMD spark-submit \
    --packages ${SPARK_PACKAGES} \
    --master local[*] \
    --class es.upm.dit.ging.predictor.MakePrediction \
    --conf spark.driver.extraJavaOptions="-Dlog4j.rootCategory=WARN,console" \
    /app/flight_prediction.jar
