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


Sample of streaming microbatches - - -
```
04:43 INFO WriteToDataSourceV2Exec: Data source write support MicroBatchWrite[epoch: 2, writer: ConsoleWriter[numRows=20, truncate=true]] is committing.
-------------------------------------------
Batch: 2
-------------------------------------------
+--------------------+---------------------------------+----------------+--------+--------------+---------+
|                 url|                            title|        seendate|language| sourcecountry|sentiment|
+--------------------+---------------------------------+----------------+--------+--------------+---------+
|https://news.yaho...|             Kimberly Guilfoyl...|20241227T001500Z| English| United States|      0.0|
|https://www.yahoo...|             Kimberly Guilfoyl...|20241226T213000Z| English| United States|      0.0|
|https://www.hltv....|             Forum thread : sl...|20241227T193000Z| English| United States|      0.0|
|https://www.daily...|             Barron surprise c...|20241224T180000Z| English|United Kingdom|      0.0|
|https://www.insig...|   정용진 , 트럼프 당선인과 10...|20241222T043000Z|  Korean|   South Korea|      0.0|
|http://news.nate....|   정용진 ,  한국패싱  트럼프 ...|20241222T014500Z|  Korean| United States|      0.0|
|https://news.mt.c...| 정용진  트럼프와 심도 깊은 대...|20241222T040000Z|  Korean|   South Korea|      0.0|
|https://www.newsw...|             Toyota Joins Ford...|20241225T000000Z| English| United States|      0.0|
|http://segye.com/...|   트럼프 만난 정용진 …  여러 ...|20241222T113000Z|  Korean|   South Korea|      0.0|
|https://navbharat...|             Ivanka Trump Late...|20241222T154500Z|   Hindi|         India|      0.0|
|https://www.bhask...|             Why Trump Wants G...|20241225T023000Z|   Hindi|         India|      0.0|
|http://www.koreat...|   곧 취임하는데 … 트럼프 , 굿...|20241226T083000Z|  Korean|   South Korea|      0.0|
|https://www.hanko...|   정용진 , 미국서 트럼프와 15...|20241222T014500Z|  Korean|   South Korea|      0.0|
|http://segye.com/...|   양안 인식 드러낸 트럼프 …  ...|20241222T113000Z|  Korean|   South Korea|      0.0|
|https://www.forbe...|             Trump Biggest Cab...|20241226T161500Z| English| United States|      0.0|
|https://news.ifen...|美前国家安全顾问 ： 非常担心  ...|20241225T123000Z| Chinese|         China|      0.0|
|https://sabq.org/...|               أنا قلق جدًا .....|20241224T183000Z|  Arabic|  Saudi Arabia|      0.0|
|https://indianexp...|             Trump speech , Pa...|20241223T033000Z| English|         India|      0.0|
|https://www.yahoo...|             Ford , General Mo...|20241224T133000Z| English| United States|      0.0|
|https://www.liveh...|             Elon Musk to be B...|20241223T034500Z|   Hindi|         India|      0.0|
+--------------------+---------------------------------+----------------+--------+--------------+---------+
only showing top 20 rows

```