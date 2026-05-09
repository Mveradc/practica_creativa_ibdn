FROM spark:4.1.1-scala2.13-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_PACKAGES="com.datastax.oss:java-driver-core:4.17.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:$PATH

RUN set -ex; \
    curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.1.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.1/spark-sql-kafka-0-10_2.13-4.1.1.jar; \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.1.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.1/spark-token-provider-kafka-0-10_2.13-4.1.1.jar; \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.9.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar; \
    curl -fsSL -o /opt/spark/jars/java-driver-core-4.17.0.jar https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.17.0/java-driver-core-4.17.0.jar; \
    curl -fsSL -o /opt/spark/jars/java-driver-shaded-guava-25.1-jre-graal-sub-1.jar https://repo1.maven.org/maven2/com/datastax/oss/java-driver-shaded-guava/25.1-jre-graal-sub-1/java-driver-shaded-guava-25.1-jre-graal-sub-1.jar; \
    curl -fsSL -o /opt/spark/jars/native-protocol-1.5.1.jar https://repo1.maven.org/maven2/com/datastax/oss/native-protocol/1.5.1/native-protocol-1.5.1.jar; \
    curl -fsSL -o /opt/spark/jars/typesafe-config-1.4.1.jar https://repo1.maven.org/maven2/com/typesafe/config/1.4.1/config-1.4.1.jar; \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.12.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar; \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-3.4.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar; \
    curl -fsSL -o /opt/spark/jars/aws-sdk-v2-bundle-2.29.52.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar

# Directorio de trabajo
WORKDIR /app

# Copiar ficheros que usa spark los jobs
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
