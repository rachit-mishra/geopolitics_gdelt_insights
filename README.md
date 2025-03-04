# Geopolitics Streaming Project

This repository demonstrates an end-to-end distributed streaming system using open source tools for real-time geopolitical event analysis. The system leverages:

- **Kafka** for data ingestion.
- **Spark Structured Streaming** for real-time processing.
- **Hugging Face Transformers** for AI-powered sentiment analysis.
- **ScyllaDB** (or optionally, Parquet files with DuckDB) for data storage.

The source data is fetched from a free API (for example, GDELT or a similar geopolitics news API).

## Repository Structure

- **producer/**: Contains the Kafka producer code that fetches data from the API and pushes it into Kafka.
- **spark_streaming/**: Contains the Spark Structured Streaming job that processes the data, applies AI enrichment, and writes to the storage layer.
- **docs/**: Documentation for the project architecture and setup instructions.
- **docker/**: Docker configurations (e.g., docker-compose) for spinning up necessary services like Kafka and Zookeeper.

## Getting Started

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/geopolitics-streaming-project.git
   cd geopolitics-streaming-project
