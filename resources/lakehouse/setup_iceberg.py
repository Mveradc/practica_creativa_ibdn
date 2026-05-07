from pyspark.sql import SparkSession
import os

ICEBERG_ARTIFACT = os.environ.get(
    "ICEBERG_ARTIFACT",
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
)
HADOOP_AWS_ARTIFACT = os.environ.get(
    "HADOOP_AWS_ARTIFACT",
    "org.apache.hadoop:hadoop-aws:3.4.2",
)

os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {ICEBERG_ARTIFACT},{HADOOP_AWS_ARTIFACT} pyspark-shell"

# Configuración de la Sesión de Spark
spark = SparkSession.builder \
    .appName("IcebergProcessing") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://lakehouse/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .getOrCreate()

# PROCESAMIENTO DE VALORES: 
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.connection.timeout", "60000")
hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")

# Lectura y Escritura
path_source = "s3a://lakehouse/training-data/simple_flight_delay_features.jsonl.bz2"

df = spark.read.json(path_source)

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
df.writeTo("local.db.vuelos").using("iceberg").createOrReplace()

print("Procesamiento completado: La tabla Iceberg ha sido creada con éxito.")

"""
COMANDOS:

--- Creación de tablas ---

docker compose exec spark-master bash -lc "
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2 \
    /app/setup_iceberg.py
"

--- Comprobación de tablas (info) ---

docker compose exec spark-master bash -lc "
  spark-sql \
    --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=s3a://lakehouse/warehouse \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    -e 'SHOW NAMESPACES IN local; SHOW TABLES IN local.db; SELECT COUNT(*) AS total FROM local.db.vuelos;'
"

--- Comprobación de tablas (conteo) ---

docker compose exec spark-master bash -lc "
  spark-sql \
    --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.2 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=s3a://lakehouse/warehouse \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    -e 'SELECT COUNT(*) AS total FROM local.db.vuelos;'
"

"""
