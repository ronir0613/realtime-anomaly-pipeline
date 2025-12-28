from fastapi import FastAPI
from elasticsearch import Elasticsearch

app = FastAPI()

es = Elasticsearch("http://elasticsearch:9200")

@app.get("/")
def root():
    return {"message": "Anomaly Detection API running"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/anomalies")
def get_anomalies(limit: int = 20):
    res = es.search(
        index="anomalies",
        size=limit,
        sort="timestamp:desc"
    )
    return [
        {
            "sensor_id": hit["_source"]["sensor_id"],
            "value": hit["_source"]["value"],
            "timestamp": hit["_source"]["timestamp"],
            "anomaly_type": hit["_source"]["anomaly_type"]
        }
        for hit in res["hits"]["hits"]
    ]
