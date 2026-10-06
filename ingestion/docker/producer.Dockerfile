FROM python:3.12-slim

WORKDIR /app

COPY ingestion/docker/producer-requirements.txt .

RUN pip install --no-cache-dir -r producer-requirements.txt

COPY ingestion/kafka_producer.py /app/kafka_producer.py

CMD ["python", "/app/kafka_producer.py"]