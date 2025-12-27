from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaSparkJSON") \
    .getOrCreate()

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()


query.awaitTermination()
