package es.upm.dit.ging.predictor
import com.datastax.oss.driver.api.core.CqlSession
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{concat, from_json, lit}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.net.InetSocketAddress

object MakePrediction {

  private val CassandraKeyspace = "agile_data_science"
  private val CassandraTable = "flight_delay_ml_response"

  private def writeBatchToCassandra(batchDf: DataFrame): Unit = {
    val cassandraHost = "cassandra"
    val cassandraPort = 9042
    val cassandraDatacenter = "datacenter1"
    val insertStatement =
      s"""
         |INSERT INTO $CassandraKeyspace.$CassandraTable (
         |  uuid, origin, dest, carrier, flight_date, dep_delay, distance,
         |  day_of_week, day_of_year, day_of_month, timestamp, prediction
         |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         |""".stripMargin

    if (!batchDf.isEmpty) {
      val session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
        .withLocalDatacenter(cassandraDatacenter)
        .withKeyspace(CassandraKeyspace)
        .build()

      try {
        val preparedStatement = session.prepare(insertStatement)
        val rows = batchDf.toLocalIterator()
        while (rows.hasNext) {
          val row = rows.next()
          session.execute(preparedStatement.bind(
            row.getAs[String]("uuid"),
            row.getAs[String]("origin"),
            row.getAs[String]("dest"),
            row.getAs[String]("carrier"),
            row.getAs[String]("flight_date"),
            java.lang.Double.valueOf(row.getAs[Double]("dep_delay")),
            java.lang.Double.valueOf(row.getAs[Double]("distance")),
            java.lang.Integer.valueOf(row.getAs[Int]("day_of_week")),
            java.lang.Integer.valueOf(row.getAs[Int]("day_of_year")),
            java.lang.Integer.valueOf(row.getAs[Int]("day_of_month")),
            row.getAs[String]("timestamp"),
            java.lang.Double.valueOf(row.getAs[Double]("prediction"))
          ))
        }
      } finally {
        session.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println("Fligth predictor starting...")

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
      .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
      .getOrCreate()
    import spark.implicits._

    //Load the arrival delay bucketizer from MinIO
    val base_path= "/app"
    
    val arrivalBucketizerPath = "s3a://lakehouse/models/arrival_bucketizer_2.0.bin"
    print(arrivalBucketizerPath.toString())
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)
    val columns= Seq("Carrier","Origin","Dest","Route")

    //Load all the string field vectorizer pipelines into a dict
    val stringIndexerModelPath = columns.map { n =>
      val path = "s3a://lakehouse/models/string_indexer_model_%s.bin".format(n)
      path
    }
    val stringIndexerModel = stringIndexerModelPath.map{n => StringIndexerModel.load(n.toString)}
    val stringIndexerModels  = (columns zip stringIndexerModel).toMap

    // Load the numeric vector assembler
    val vectorAssemblerPath = "s3a://lakehouse/models/numeric_vector_assembler.bin"
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    // Load the classifier model
    val randomForestModelPath = "s3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    //Process Prediction Requests in Streaming
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "flight-delay-ml-request")
      .load()
    df.printSchema()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json($"value", struct).as("flight"))
    flightNestedDf.printSchema()

    // DataFrame for Vectorizing string fields with the corresponding pipeline for that column
    val flightFlattenedDf = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance")
    flightFlattenedDf.printSchema()

    val predictionRequestsWithRouteMod = flightFlattenedDf.withColumn(
      "Route",
                concat(
                  flightFlattenedDf("Origin"),
                  lit('-'),
                  flightFlattenedDf("Dest")
                )
    )

    // Dataframe for Vectorizing numeric columns
    val flightFlattenedDf2 = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance",
      "flight.Carrier_index","flight.Origin_index","flight.Dest_index","flight.Route_index")
    flightFlattenedDf2.printSchema()

    val predictionRequestsWithRouteMod2 = flightFlattenedDf2.withColumn(
      "Route",
      concat(
        flightFlattenedDf2("Origin"),
        lit('-'),
        flightFlattenedDf2("Dest")
      )
    )

    // Vectorize string fields with the corresponding pipeline for that column
    // Turn category fields into categoric feature vectors, then drop intermediate fields
    val predictionRequestsWithRoute = stringIndexerModel.map(n=>n.transform(predictionRequestsWithRouteMod))

    //Vectorize numeric columns: DepDelay, Distance and index columns
    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(predictionRequestsWithRouteMod2)

    // Inspect the vectors
    vectorizedFeatures.printSchema()

    // Drop the individual index columns
    val finalVectorizedFeatures = vectorizedFeatures
        .drop("Carrier_index")
        .drop("Origin_index")
        .drop("Dest_index")
        .drop("Route_index")

    // Inspect the finalized features
    finalVectorizedFeatures.printSchema()

    // Make the prediction
    val predictions = rfc.transform(finalVectorizedFeatures)
      .drop("Features_vec")

    // Drop the features vector and prediction metadata to give the original fields
    val finalPredictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")

    // Inspect the output
    finalPredictions.printSchema()

    // define a streaming query
    val cassandraPredictions = finalPredictions.selectExpr(
      "CAST(UUID AS STRING) AS uuid",
      "CAST(Origin AS STRING) AS origin",
      "CAST(Dest AS STRING) AS dest",
      "CAST(Carrier AS STRING) AS carrier",
      "CAST(FlightDate AS STRING) AS flight_date",
      "CAST(DepDelay AS DOUBLE) AS dep_delay",
      "CAST(Distance AS DOUBLE) AS distance",
      "CAST(DayOfWeek AS INT) AS day_of_week",
      "CAST(DayOfYear AS INT) AS day_of_year",
      "CAST(DayOfMonth AS INT) AS day_of_month",
      "CAST(Timestamp AS STRING) AS timestamp",
      "CAST(prediction AS DOUBLE) AS prediction"
    )

    val cassandraQuery = cassandraPredictions
      .writeStream
      .foreachBatch((batchDf: DataFrame, _: Long) => writeBatchToCassandra(batchDf))
      .option("checkpointLocation", "/tmp/flight-delay-ml-cassandra-checkpoint")
      .outputMode("append")
      .start()

    val kafkaPredictions = finalPredictions.selectExpr(
      "CAST(UUID AS STRING) AS key",
      "to_json(struct(*)) AS value"
    )

    val kafkaQuery = kafkaPredictions
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "flight-delay-ml-results")
      .option("checkpointLocation", "/tmp/flight-delay-ml-results-checkpoint")
      .outputMode("append")
      .start()

    spark.streams.awaitAnyTermination()
  }

}
