
---

## 3. Producer Module

### producer/kafka_producer.py

```python
import json
import time
import requests
from kafka import KafkaProducer

# Configure Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# API endpoint for geopolitical events (adjust as needed)
API_URL = "https://api.example.com/articles?query=geopolitics&format=json"

def fetch_articles():
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            # Assume the API returns a key "articles" with a list of articles.
            articles = data.get("articles", [])
            return articles
        else:
            print("Error fetching data:", response.status_code)
    except Exception as e:
        print("Exception during API request:", e)
    return []

def main():
    while True:
        articles = fetch_articles()
        print(f"Fetched {len(articles)} articles.")
        for article in articles:
            producer.send('geopolitics_events', article)
        producer.flush()
        # Poll every minute
        time.sleep(60)

if __name__ == '__main__':
    main()
