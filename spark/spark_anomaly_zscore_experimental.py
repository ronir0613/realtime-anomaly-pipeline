from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, avg, stddev, abs as spark_abs
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (
    SparkSession.builder
    .appName("KafkaSparkAnomaly")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("value", DoubleType()),
    StructField("timestamp", StringType())
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor-data")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("data"))
       .select("data.*")
)

stats = (
    parsed.groupBy("sensor_id")
    .agg(
        avg("value").alias("mean"),
        stddev("value").alias("std")
    )
)

joined = parsed.join(stats, on="sensor_id")

anomalies = (
    joined
    .withColumn("z_score", spark_abs((col("value") - col("mean")) / col("std")))
    .filter(col("z_score") > 2.5)
)

query = (
    anomalies.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
