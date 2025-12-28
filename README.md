# Real-Time Anomaly Detection Pipeline

## Overview

This project implements a **real-time anomaly detection pipeline** using **Apache Kafka**, **Apache Spark Structured Streaming**, **Elasticsearch**, and **FastAPI**.  
It simulates streaming sensor data, detects anomalies in real time, stores them, and exposes them via a REST API.

The system is fully containerized using **Docker Compose**.

---

## Architecture

```
Producer (Python)
      ↓
Kafka (sensor-data topic)
      ↓
Spark Structured Streaming
      ↓
Elasticsearch (anomalies index)
      ↓
FastAPI (REST API)
```

---

## Technologies Used

- **Apache Kafka** – real-time message streaming  
- **Apache Spark (Structured Streaming)** – stream processing & anomaly detection  
- **Elasticsearch** – anomaly storage and querying  
- **FastAPI** – REST API layer  
- **Docker & Docker Compose** – container orchestration  
- **Python** – producer, Spark job, and API  

---

## Project Structure

```
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
│   ├── spark_anamaly_zscore_experimental.py
│   └── requirements.txt
│
├── api/
│   ├── app.py
│   └── requirements.txt
│
└── README.md
```

---

## Anomaly Detection Logic (Approach A – Rule Based)

A sensor reading is classified as an anomaly if:

- **HIGH anomaly** → value > 90  
- **LOW anomaly** → value < 10  

This approach is lightweight, fast, easy to interpret, and suitable for real-time and academic projects.

---

## How the System Works

### Producer
- Generates simulated sensor data every 2 seconds  
- Sends JSON messages to Kafka topic `sensor-data`  

### Kafka
- Buffers incoming sensor data  
- Acts as the streaming backbone  

### Spark Structured Streaming
- Reads data from Kafka  
- Parses JSON messages  
- Applies anomaly detection rules  
- Writes detected anomalies to Elasticsearch  

### Elasticsearch
- Stores anomalies in the `anomalies` index  
- Enables fast querying  

### FastAPI
- Exposes REST endpoints to fetch anomalies  
- Connects directly to Elasticsearch  

---

## How to Run the Project

### Start all services
```
docker compose up -d
```

### Start the Kafka Producer
```
docker run --rm -it --network realtime-anomaly-pipeline_default `
  -v ${PWD}/producer:/app `
  python:3.9-slim bash
```

```
pip install -r /app/requirements.txt
python /app/producer.py
```
## Check Kafka Consumer
```
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic sensor-data
```

### Run the Spark Streaming Job
```
docker exec -it spark-master bash

/opt/spark/bin/spark-submit   --master spark://spark-master:7077   --conf spark.executor.cores=1   --conf spark.executor.memory=512m   --conf spark.cores.max=1   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0   /opt/spark/work-dir/spark_streaming.py
```

### API Endpoints

- Health check: `http://localhost:8000/health`  
- Fetch anomalies: `http://localhost:8000/anomalies`  

---

## Example Anomaly Output

```
{
  "sensor_id": "sensor_1",
  "value": 95.4,
  "timestamp": "2025-12-27T10:55:29.614440",
  "anomaly_type": "HIGH"
}
```

---

## Future Enhancements

- ML-based anomaly detection  
- Kibana dashboards  
- Multiple sensors  
- Alerting system  

---

## Conclusion

This project demonstrates a scalable real-time streaming architecture for anomaly detection using modern data engineering tools.
