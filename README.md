Real-Time Anomaly Detection Pipeline
Overview

This project implements a real-time anomaly detection pipeline using Apache Kafka, Apache Spark Structured Streaming, Elasticsearch, and FastAPI.
It simulates streaming sensor data, detects anomalies in real time, stores them, and exposes them via a REST API.

The system is fully containerized using Docker Compose.

Architecture
Producer (Python)
      ↓
Kafka (sensor-data topic)
      ↓
Spark Structured Streaming
      ↓
Elasticsearch (anomalies index)
      ↓
FastAPI (REST API)

Technologies Used

Apache Kafka – real-time message streaming

Apache Spark (Structured Streaming) – stream processing & anomaly detection

Elasticsearch – anomaly storage and querying

FastAPI – REST API layer

Docker & Docker Compose – container orchestration

Python – producer, Spark job, and API

Project Structure
realtime-anomaly-pipeline/
│
├── docker-compose.yml
│
├── producer/
│   ├── producer.py
│   └── requirements.txt
│
├── spark/
│   ├── spark_streaming.py
│   └── requirements.txt
│
├── api/
│   ├── app.py
│   └── requirements.txt
│
└── README.md

Anomaly Detection Logic (Approach A – Rule Based)

A sensor reading is classified as an anomaly if:

HIGH anomaly → value > 90

LOW anomaly → value < 10

This approach is:

Lightweight and fast

Easy to interpret

Suitable for real-time systems and academic projects

How the System Works
1. Producer

Generates simulated sensor data every 2 seconds

Sends JSON messages to Kafka topic sensor-data

2. Kafka

Buffers incoming sensor data

Acts as the streaming backbone

3. Spark Structured Streaming

Reads data from Kafka

Parses JSON messages

Applies anomaly detection rules

Writes detected anomalies to Elasticsearch

4. Elasticsearch

Stores anomalies in the anomalies index

Enables fast querying and sorting

5. FastAPI

Provides REST endpoints to access anomalies

Connects directly to Elasticsearch

How to Run the Project
Step 1: Start all services
docker compose up -d

Step 2: Start the Kafka Producer
docker exec -it kafka bash
python producer.py

Step 3: Run the Spark Streaming Job
docker exec -it spark-master bash

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=512m \
  --conf spark.cores.max=1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
  /opt/spark/work-dir/spark_streaming.py

Step 4: Access the API

Health check

http://localhost:8000/health


Fetch recent anomalies

http://localhost:8000/anomalies

Example Anomaly Output
{
  "sensor_id": "sensor_1",
  "value": 95.4,
  "timestamp": "2025-12-27T10:55:29.614440",
  "anomaly_type": "HIGH"
}

Future Enhancements

Statistical or ML-based anomaly detection (Z-score, Isolation Forest)

Kibana dashboard for visualization

Multiple sensors support

Alerting via email or notifications

Conclusion

This project demonstrates a scalable, real-time streaming architecture for anomaly detection using industry-standard tools.