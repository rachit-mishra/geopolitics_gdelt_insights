## 4. Spark Streaming Module

### spark_streaming/spark_streaming.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Define an example schema for the articles (adjust fields as needed)
article_schema = StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("seendate", StringType(), True),
    StructField("language", StringType(), True),
    StructField("sourcecountry", StringType(), True)
])

# Initialize Spark Session
spark = SparkSession.builder.appName("GeopoliticalStreaming").getOrCreate()

# Read from Kafka topic
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "geopolitics_events") \
    .load()

# Convert binary value to string and parse JSON
articles_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), article_schema).alias("data")) \
    .select("data.*")

# AI Integration: sentiment analysis using Hugging Face Transformers
from transformers import pipeline

# Global variable to lazy-load the model
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

# Register UDF
sentiment_udf = udf(analyze_sentiment, FloatType())

# Enrich the DataFrame with sentiment analysis
# (Using title text for analysis; adjust field as needed)
enriched_df = articles_df.withColumn("sentiment", sentiment_udf(col("title")))

# Write the enriched stream to the console (or change to write to ScyllaDB/Parquet)
query = enriched_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
