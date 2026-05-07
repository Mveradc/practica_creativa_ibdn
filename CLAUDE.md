# Flight Delay Prediction System - Practica Creativa

## Project Overview
Real-time flight delay prediction system built with PySpark, Spark Streaming, Kafka, and MongoDB. Based on "Agile Data Science 2.0" (O'Reilly).

**Stack:**
- **Backend:** Scala, Spark MLlib, Spark Streaming, Kafka (KRaft)
- **Frontend:** Flask (Python), JavaScript
- **Storage:** MongoDB 7.0, Cassandra (for distances)
- **Deployment:** Docker, Docker Compose
- **Build:** SBT

## Architecture

### Backend Pipelines
1. **Batch Training** (`train_spark_mllib_model.py`)
   - Trains classification model on historical flight data (2015)
   - Saves model to `models/` directory
   - Uses PySpark MLlib

2. **Real-time Prediction** (`MakePrediction.scala`)
   - Loads trained model
   - Listens on Kafka topic: `flight-delay-ml-request`
   - Stores predictions in MongoDB collection: `flight_delay_ml_response`

3. **Data Import**
   - `import_distances.sh`: loads distance records into MongoDB
   - `download_data.sh`: fetches raw flight datasets

### Frontend
- Flask app (`predict_flask.py`) on `localhost:5000`
- Prediction form: `/flights/delays/predict_kafka`
- Response polling from: `/flights/delays/predict/classify_realtime/response/`

## Recent Work
- **Lakehouse with Apache Iceberg** integration (beta)
- **Cassandra distances** implementation
- **Spark cluster mode** with WebSocket simplification
- Training with lakehouse architecture

## Key Setup Steps
1. Set `JAVA_HOME` (jdk 17), `SPARK_HOME`, `PROJECT_HOME`
2. Start MongoDB: `docker run --name mongo -d -p 27017:27017 mongo:7.0.17`
3. Initialize & start Kafka KRaft cluster (Spark 4.1.1, Scala 2.13)
4. Import distances: `./resources/import_distances.sh`
5. Train model: `python3 resources/train_spark_mllib_model.py .`
6. Run predictor: IntelliJ or `spark-submit` with mongo-spark-connector + spark-sql-kafka packages
7. Start Flask: `cd practica_creativa/resources/web && python3 predict_flask.py`

## Key Files
- `flight_prediction/src/main/scala/es/upm/dit/ging/predictor/MakePrediction.scala` - Spark Streaming predictor
- `resources/train_spark_mllib_model.py` - Model training
- `resources/web/predict_flask.py` - Web frontend
- `docker-compose.yml` - Container orchestration
- `flight_prediction/build.sbt` - Scala build config
- `requirements.txt` - Python dependencies

## Dependencies
- **Spark Packages:** `org.mongodb.spark:mongo-spark-connector_2.12:10.4.1`, `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`
- **Python:** Flask, PySpark, MLlib

## How to Get Help
- Check chapters 7-8 of "Agile Data Science 2.0" for architecture details
- Consult O'Reilly book for Kafka + Spark Streaming setup
- Review `docker-compose.yml` for container dependencies

## Pending Work
- Apache Airflow DAG execution and monitoring (optional for final submission)
- Lakehouse optimizations with Iceberg
