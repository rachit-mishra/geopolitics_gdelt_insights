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

- Python 3.8+
- Docker and Docker Compose
- Required Python packages (auto-installed by startup script):
  - kafka-python
  - requests
  - pyspark
  - transformers
  - cassandra-driver
  - pandas

## Quick Start

1. Clone the repository:
```bash
git clone <repository-url>
cd geopolitics_gdelt_insights
```

2. Start the system:
```bash
python startup.py --topic geopolitics_events --search "your_search_term"
```

3. Monitor analytics:
```bash
python analytics/scylla_analytics.py
```

4. To shut down:
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
- `startup.log`: System initialization logs
- `producer.log`: GDELT API fetching logs
- `spark_streaming.log`: Processing logs
- `analytics.log`: Analytics and insights logs

### Health Checks
- Component status monitoring
- Data pipeline monitoring
- Error rate tracking

## Troubleshooting

### Common Issues
1. **Connection Issues**
   - Verify Docker containers are running
   - Check network connectivity
   - Ensure correct ports are exposed

2. **Data Pipeline Issues**
   - Check Kafka topic existence
   - Verify ScyllaDB table schema
   - Monitor Spark streaming logs

3. **Performance Issues**
   - Monitor resource usage
   - Check ScyllaDB write performance
   - Verify Spark processing latency

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