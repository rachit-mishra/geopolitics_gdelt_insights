# Spark Streaming Module

This module reads articles from a Kafka topic, parses the JSON data, applies sentiment analysis using Hugging Face Transformers, and writes the enriched data to the console (or your desired sink).

## How to Run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
