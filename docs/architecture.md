# Architecture Overview

This project is an end-to-end distributed streaming system designed for analyzing geopolitical events. The architecture consists of:

1. **Data Ingestion (Producer):**
   - Polls a geopolitical news API.
   - Sends JSON messages to a Kafka topic (`geopolitics_events`).

2. **Stream Processing (Spark Streaming):**
   - Reads messages from Kafka.
   - Parses the JSON and applies sentiment analysis using Hugging Face Transformers.
   - Enriches the data with additional fields (e.g., sentiment score).

3. **Data Storage:**
   - Writes the processed data to ScyllaDB using the Spark Cassandra connector.
   - Alternatively, data can be saved as Parquet files for use with DuckDB.

4. **(Optional) Docker:**
   - Contains configurations to spin up Kafka, Zookeeper, and other services.

See [docs/setup.md](docs/setup.md) for detailed setup instructions.
