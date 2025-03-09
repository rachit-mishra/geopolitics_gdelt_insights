# Geopolitics GDELT Insights Startup Guide

This document explains how to use the `startup.py` script to launch the entire Geopolitics GDELT Insights pipeline.

## Prerequisites

Before running the startup script, ensure you have:

1. Docker installed and configured
2. Docker Compose installed
3. Python 3.7+ installed
4. Virtual environment set up with all dependencies installed

## Docker Setup

The project uses Docker to run Kafka and ScyllaDB. The Docker Compose file is located at `docker/docker-compose.yml`.

You can manually start the containers with:

```bash
cd docker
docker-compose up -d
```

However, the startup script will automatically check if the containers are running and start them if needed.

## Running the Startup Script

The startup script handles:

1. Checking if Docker is running and starting it if needed (on macOS)
2. Verifying that required containers (Kafka and ScyllaDB) are running
3. Validating the ScyllaDB setup
4. Starting the Kafka producer with the specified topic and search term
5. Starting the Spark streaming job
6. Monitoring all processes and providing graceful shutdown

### Basic Usage

```bash
python startup.py
```

This will start the pipeline with default settings:
- Topic name: `geopolitics_events`
- Search term: `ukraine`

### Advanced Usage

You can customize the topic name and search term:

```bash
python startup.py --topic custom_topic --search-term "climate change"
```

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--topic` | Kafka topic name to produce to and consume from | `geopolitics_events` |
| `--search-term` | Search term for GDELT API | `ukraine` |
| `--skip-docker-check` | Skip Docker and container checks | `false` |

## Monitoring

The startup script creates log files:
- `startup.log` - Logs from the startup script itself
- `producer.log` - Logs from the Kafka producer
- `spark.log` - Logs from the Spark streaming job

You can monitor these logs to track the progress of the pipeline.

## Stopping the Pipeline

To stop the pipeline, press `Ctrl+C` in the terminal where the startup script is running. The script will gracefully terminate all processes.

## Troubleshooting

If you encounter issues:

1. Check the log files for error messages
2. Verify that Docker is running
3. Ensure that Kafka and ScyllaDB containers are running
4. Check that the virtual environment is properly set up
5. Verify network connectivity for the GDELT API

## Project Structure

- `startup.py` - Main script to start the pipeline
- `producer/kafka_producer.py` - Kafka producer for GDELT articles
- `spark_streaming/spark_streaming.py` - Spark streaming job for processing articles
- `check_scylla.py` - Script to validate ScyllaDB setup
- `analytics/scylla_analytics.py` - Analytics for processed articles 