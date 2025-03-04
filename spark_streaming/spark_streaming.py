### spark_streaming/spark_streaming.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType

article_schema = StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("seendate", StringType(), True),
    StructField("language", StringType(), True),
    StructField("sourcecountry", StringType(), True)
])


spark = SparkSession.builder.appName("GeopoliticalStreaming").getOrCreate()


kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "geopolitics_events") \
    .load()

articles_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), article_schema).alias("data")) \
    .select("data.*")


from transformers import pipeline


sentiment_pipeline = None

def get_sentiment_pipeline():
    global sentiment_pipeline
    if sentiment_pipeline is None:
        sentiment_pipeline = pipeline("sentiment-analysis")
    return sentiment_pipeline

def analyze_sentiment(text):
    try:
        pipe = get_sentiment_pipeline()
        result = pipe(text)
        label = result[0]['label']
        score = result[0]['score']
        return float(score) if label.upper() == "POSITIVE" else -float(score)
    except Exception as e:
        return 0.0

sentiment_udf = udf(analyze_sentiment, FloatType())


enriched_df = articles_df.withColumn("sentiment", sentiment_udf(col("title")))

# Write the enriched stream to the console (or change to write to ScyllaDB/Parquet)
query = enriched_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
