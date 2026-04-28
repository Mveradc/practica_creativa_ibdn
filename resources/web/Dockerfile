FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY predict_flask.py .
COPY predict_utils.py .
COPY config.py .
COPY static/ ./static/
COPY templates/ ./templates/

# Variable de entorno que necesita predict_flask.py
ENV PROJECT_HOME=/app
ENV KAFKA_BROKERS=localhost:9092
ENV MONGO_URI=mongodb://localhost:27017

EXPOSE 5001

CMD ["python3", "predict_flask.py"]
