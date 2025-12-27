import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    data = {
        "sensor_id": "sensor_1",
        "value": round(random.normalvariate(70, 5), 2),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send("sensor-data", data)
    print("Sent:", data)
    time.sleep(1)
