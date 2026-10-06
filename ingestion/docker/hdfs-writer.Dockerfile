FROM python:3.12-slim

WORKDIR /app

COPY ingestion/docker/hdfs-writer-requirements.txt .

RUN pip install --no-cache-dir -r hdfs-writer-requirements.txt

COPY ingestion/hdfs_writer.py /app/hdfs_writer.py

CMD ["python", "/app/hdfs_writer.py"]