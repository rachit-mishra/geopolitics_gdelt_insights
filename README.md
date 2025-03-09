# Geopolitics GDELT Insights

A real-time geopolitical event analysis system that processes news articles from the GDELT Project, performs sentiment analysis, and provides insights into global events.

## Architecture

```
┌─────────────┐    ┌─────────┐    ┌──────────────┐    ┌──────────┐
│ GDELT API   │ -> │ Kafka   │ -> │ Spark        │ -> │ ScyllaDB │
│ (Producer)  │    │ Topic   │    │ Streaming    │    │          │
└─────────────┘    └─────────┘    └──────────────┘    └──────────┘
                                         │
                                         v
                                  ┌──────────────┐
                                  │ Sentiment    │
                                  │ Analysis     │
                                  └──────────────┘
```

## Prerequisites

- Python 3.8+ (tested with Python 3.11)
- Docker and Docker Compose
- 8GB RAM minimum (recommended 16GB)
- macOS, Linux, or WSL2 on Windows

## Setup Instructions

1. Clone the repository:
```bash
git clone <repository-url>
cd geopolitics_gdelt_insights
```

2. Create and activate a virtual environment:
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
.\venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Start Docker:
- On macOS/Linux: Start Docker Desktop or run `systemctl start docker`
- On Windows: Start Docker Desktop

5. Start the system:
```bash
# Make sure you're in the project root and virtual environment is activated
python startup.py --topic "your_topic_name" --search "your_search_term"
```

The startup script will:
- Check and install any missing Python packages
- Verify Docker is running
- Start required containers (Kafka, Zookeeper, ScyllaDB)
- Create Kafka topics
- Start the GDELT producer
- Initialize Spark streaming

6. Monitor the system:
```bash
# In a new terminal (don't forget to activate venv):
source venv/bin/activate  # or .\venv\Scripts\activate on Windows
python analytics/scylla_analytics.py
```

7. To shut down:
```bash
python shutdown.py
```

## Components

### 1. Data Ingestion (Producer)
- Fetches articles from GDELT API every 30 seconds
- Implements rate limiting and error handling
- Publishes articles to Kafka topic

### 2. Stream Processing
- Spark Streaming job processes articles in real-time
- Performs sentiment analysis using DistilBERT
- Handles data transformation and enrichment
- Ensures fault-tolerant processing

### 3. Data Storage
- ScyllaDB for high-performance storage
- Schema optimized for time-series analytics
- Supports efficient querying and analysis

### 4. Analytics
The analytics component provides real-time insights including:

#### Data Quality Metrics
- Total records processed
- Null/empty value distribution
- Data completeness analysis

#### Sentiment Analysis
- Distribution of positive/negative/neutral sentiments
- Sentiment quality metrics
- Cross-tabulation with countries and languages

#### Geographic Insights
- Source country distribution
- Language distribution
- Domain analysis

#### Temporal Analysis
- Hourly article distribution
- Ingestion rate monitoring
- Recent articles sampling

## Monitoring and Maintenance

### Log Files
All logs are stored in the project root:
- `startup.log`: System initialization logs
- `producer.log`: GDELT API fetching logs
- `spark_streaming.log`: Processing logs
- `analytics.log`: Analytics and insights logs

### Health Checks
You can monitor the system health:

1. Check Docker containers:
```bash
docker ps
```

2. View Kafka topics:
```bash
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

3. Check ScyllaDB:
```bash
docker exec scylla nodetool status
```

4. Monitor logs in real-time:
```bash
# Producer logs
tail -f producer.log

# Spark streaming logs
tail -f spark_streaming.log

# Analytics logs
tail -f analytics.log
```

## Troubleshooting

### Common Issues

1. **Virtual Environment Issues**
   - Ensure you're in the correct directory
   - Verify virtual environment is activated (you should see `(venv)` in your prompt)
   - Try recreating the virtual environment if packages are missing

2. **Connection Issues**
   - Verify Docker containers are running: `docker ps`
   - Check container logs: `docker logs [container_name]`
   - Ensure ports are not in use (9042 for ScyllaDB, 9092 for Kafka)

3. **Data Pipeline Issues**
   - Check Kafka topic exists: `docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092`
   - Verify ScyllaDB is accepting connections: `docker exec -it scylla cqlsh -u cassandra -p cassandra`
   - Monitor Spark streaming logs for processing errors

4. **Performance Issues**
   - Monitor system resources (CPU, memory)
   - Check ScyllaDB write performance: `docker exec scylla nodetool tpstats`
   - Adjust Spark memory settings in `spark_streaming.py` if needed

### Quick Fixes

1. **Reset the System**
```bash
# Stop everything
python shutdown.py

# Remove checkpoint directory
rm -rf checkpoint/

# Start again
python startup.py --topic geopolitics_events --search "your_search_term"
```

2. **Clear Data and Start Fresh**
```bash
# Stop containers and remove volumes
docker-compose down -v

# Start the system
python startup.py --topic geopolitics_events --search "your_search_term"
```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [GDELT Project](https://www.gdeltproject.org/) for providing the data API
- [Hugging Face](https://huggingface.co/) for the sentiment analysis models
- Apache Kafka, Apache Spark, and ScyllaDB communities