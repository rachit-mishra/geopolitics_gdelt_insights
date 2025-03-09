from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from transformers import pipeline
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import logging
import time
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spark_streaming.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define the schema for the incoming Kafka messages
article_schema = StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("seendate", StringType(), True),
    StructField("socialimage", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("language", StringType(), True),
    StructField("sourcecountry", StringType(), True)
])

# Initialize sentiment analyzer
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

def analyze_sentiment(text):
    try:
        if not text or not isinstance(text, str):
            return "UNKNOWN"
        result = sentiment_analyzer(text)
        return result[0]['label']
    except Exception as e:
        logger.error(f"Error in sentiment analysis: {e}")
        return "UNKNOWN"

def setup_scylladb():
    try:
        # Create ScyllaDB keyspace and table
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['localhost'], auth_provider=auth_provider, port=9042)
        session = cluster.connect()
        
        # Create keyspace if not exists
        logger.info("Creating keyspace...")
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS geopolitics
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            AND durable_writes = true
        """)
        
        # Use the keyspace
        session.set_keyspace('geopolitics')
        
        # Check if table exists
        rows = session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name='geopolitics' AND table_name='articles'")
        table_exists = bool(list(rows))
        
        if table_exists:
            logger.info("Table 'articles' already exists, checking for required columns...")
            # Check if processed_at column exists
            columns = session.execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name='geopolitics' AND table_name='articles'")
            column_names = {row.column_name for row in columns}
            logger.info(f"Found columns: {column_names}")
            
            if 'processed_at' not in column_names:
                logger.warning("Column 'processed_at' is missing, dropping and recreating table...")
                session.execute("DROP TABLE geopolitics.articles")
                table_exists = False
        
        if not table_exists:
            # Create table only if it doesn't exist
            logger.info("Creating table with schema...")
            session.execute("""
                CREATE TABLE articles (
                    url text,
                    processed_at timestamp,
                    title text,
                    seendate text,
                    socialimage text,
                    domain text,
                    language text,
                    sourcecountry text,
                    sentiment text,
                    PRIMARY KEY (url, processed_at)
                ) WITH CLUSTERING ORDER BY (processed_at DESC)
            """)
        
        logger.info("ScyllaDB setup completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error setting up ScyllaDB: {e}")
        raise
    finally:
        if 'session' in locals():
            session.shutdown()
        if 'cluster' in locals():
            cluster.shutdown()

# Initialize Spark session with all necessary configurations
spark = SparkSession.builder \
    .appName("GeopoliticsStreamProcessor") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," + 
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.cassandra.connection.keepAliveMS", "60000") \
    .config("spark.cassandra.connection.timeoutMS", "15000") \
    .config("spark.cassandra.output.consistency.level", "LOCAL_ONE") \
    .config("spark.cassandra.input.consistency.level", "LOCAL_ONE") \
    .config("spark.cassandra.output.batch.size.rows", "10") \
    .config("spark.cassandra.output.concurrent.writes", "1") \
    .config("spark.cassandra.output.batch.grouping.buffer.size", "100") \
    .config("spark.cassandra.output.ignoreNulls", "true") \
    .config("spark.cassandra.output.metrics", "true") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .config("spark.python.worker.memory", "1g") \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.task.maxFailures", "10") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.network.timeout", "800s") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

# Set log level to INFO
spark.sparkContext.setLogLevel("INFO")

# Register UDF for sentiment analysis
sentiment_udf = udf(analyze_sentiment, StringType())

def process_batch(df, epoch_id):
    try:
        if df.isEmpty():
            logger.info(f"Batch {epoch_id} is empty, skipping processing")
            return

        logger.info(f"Processing batch {epoch_id} with {df.count()} records")
        
        try:
            # Parse JSON from Kafka value and handle potential errors
            parsed_df = df.select(
                from_json(col("value").cast("string"), article_schema).alias("data")
            ).select("data.*")
            
            if parsed_df.isEmpty():
                logger.warning(f"No valid records found after JSON parsing in batch {epoch_id}")
                return
                
            logger.info(f"Successfully parsed {parsed_df.count()} records from Kafka")
            
            # Apply sentiment analysis to the title
            logger.info("Applying sentiment analysis...")
            df_with_sentiment = parsed_df.withColumn("sentiment", sentiment_udf(col("title")))
            
            # Add timestamp
            df_with_sentiment = df_with_sentiment.withColumn("processed_at", current_timestamp())
            
            # Ensure column names match exactly
            df_final = df_with_sentiment.select(
                col("url").cast("string"),
                col("processed_at").cast("timestamp"),
                col("title").cast("string"),
                col("seendate").cast("string"),
                col("socialimage").cast("string"),
                col("domain").cast("string"),
                col("language").cast("string"),
                col("sourcecountry").cast("string"),
                col("sentiment").cast("string")
            )
            
            record_count = df_final.count()
            logger.info(f"Prepared {record_count} records for writing to ScyllaDB")
            
            if record_count > 0:
                # Write to ScyllaDB with retry mechanism
                max_retries = 3
                retry_count = 0
                success = False
                
                while retry_count < max_retries and not success:
                    try:
                        logger.info(f"Attempting to write to ScyllaDB (attempt {retry_count + 1}/{max_retries})...")
                        
                        # Create a single connection for the batch
                        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
                        cluster = Cluster(['localhost'], auth_provider=auth_provider, port=9042)
                        session = cluster.connect('geopolitics')
                        
                        # Prepare the insert statement once
                        insert_stmt = session.prepare("""
                            INSERT INTO articles (
                                url, processed_at, title, seendate, socialimage,
                                domain, language, sourcecountry, sentiment
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """)
                        
                        # Write each row
                        rows = df_final.collect()
                        for row in rows:
                            try:
                                session.execute(insert_stmt, (
                                    row['url'], row['processed_at'], row['title'],
                                    row['seendate'], row['socialimage'], row['domain'],
                                    row['language'], row['sourcecountry'], row['sentiment']
                                ))
                                logger.info(f"Successfully wrote record with URL: {row['url']}")
                            except Exception as e:
                                logger.error(f"Error writing record: {str(e)}")
                        
                        success = True
                        logger.info(f"Successfully wrote batch {epoch_id} to ScyllaDB")
                        
                    except Exception as e:
                        retry_count += 1
                        logger.error(f"Error details for attempt {retry_count}: {str(e)}")
                        if retry_count == max_retries:
                            logger.error(f"Failed to write to ScyllaDB after {max_retries} attempts: {str(e)}")
                            raise
                        logger.warning(f"Failed to write to ScyllaDB (attempt {retry_count}/{max_retries}): {str(e)}")
                        time.sleep(5)  # Wait 5 seconds before retrying
                    finally:
                        if 'session' in locals():
                            session.shutdown()
                        if 'cluster' in locals():
                            cluster.shutdown()
            
            logger.info(f"Successfully processed batch {epoch_id}")
            
        except Exception as e:
            logger.error(f"Error processing records: {str(e)}", exc_info=True)
            raise
            
    except Exception as e:
        logger.error(f"Error in process_batch: {str(e)}", exc_info=True)
        raise

def main():
    try:
        # Setup ScyllaDB first
        logger.info("Setting up ScyllaDB...")
        setup_scylladb()
        
        # Get topic name from environment variable or use default
        topic_name = os.environ.get("KAFKA_TOPIC", "climate")
        logger.info(f"Using Kafka topic: {topic_name}")
        
        # Create checkpoint directory
        checkpoint_dir = os.path.join(os.getcwd(), "checkpoint")
        os.makedirs(checkpoint_dir, exist_ok=True)
        logger.info(f"Created checkpoint directory at {checkpoint_dir}")
        
        while True:
            try:
                logger.info("Starting Spark Streaming...")
                # Read from Kafka
                streaming_df = spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("subscribe", topic_name) \
                    .option("startingOffsets", "latest") \
                    .option("failOnDataLoss", "false") \
                    .option("maxOffsetsPerTrigger", "100") \
                    .option("kafka.consumer.pollTimeoutMs", "5000") \
                    .option("kafka.request.timeout.ms", "60000") \
                    .option("kafka.session.timeout.ms", "30000") \
                    .option("kafka.heartbeat.interval.ms", "10000") \
                    .load()
                
                # Process the stream
                query = streaming_df.writeStream \
                    .foreachBatch(process_batch) \
                    .outputMode("update") \
                    .option("checkpointLocation", checkpoint_dir) \
                    .trigger(processingTime="10 seconds") \
                    .start()
                
                logger.info("Spark Streaming started successfully")
                
                # Monitor the query
                while query.isActive:
                    try:
                        query.awaitTermination(10)  # Check every 10 seconds
                    except Exception as e:
                        logger.error(f"Error in streaming query: {e}", exc_info=True)
                        if "py4j.protocol.Py4JError" in str(e) or "py4j.Py4JException" in str(e):
                            logger.error("Py4J error detected, restarting streaming context...")
                            break
                        time.sleep(10)  # Wait before retrying
                
                # If we get here, the query is no longer active
                logger.error("Query is not active, cleaning up and restarting...")
                
                # Clean up resources
                try:
                    if query is not None:
                        query.stop()
                except:
                    pass
                
                try:
                    # Clean up checkpoint directory
                    import shutil
                    shutil.rmtree(checkpoint_dir, ignore_errors=True)
                    os.makedirs(checkpoint_dir, exist_ok=True)
                except:
                    pass
                
                # Wait before restarting
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in streaming context: {e}", exc_info=True)
                time.sleep(10)  # Wait before retrying
                
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
