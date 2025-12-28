from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (
    SparkSession.builder
    .appName("KafkaSparkAnomalyDetection")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor-data")
    .option("startingOffsets", "latest")
    .load()
)

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

anomalies = (
    parsed
    .withColumn(
        "anomaly_type",
        when(col("value") > 90, "HIGH")
        .when(col("value") < 10, "LOW")
    )
    .filter(col("anomaly_type").isNotNull())
)

query = (
    anomalies.writeStream
    .format("org.elasticsearch.spark.sql")
    .option("checkpointLocation", "/tmp/checkpoints/anomalies")
    .option("es.nodes", "elasticsearch")
    .option("es.port", "9200")
    .option("es.resource", "anomalies/_doc")
    .outputMode("append")
    .start()
)

query.awaitTermination()
