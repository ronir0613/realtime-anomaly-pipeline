from fastapi import FastAPI
from elasticsearch import Elasticsearch

app = FastAPI()

es = Elasticsearch("http://elasticsearch:9200")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/es")
def es_check():
    return es.info()
