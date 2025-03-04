import json
import time
import requests
from kafka import KafkaProducer

# Configure Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the search term and construct the GDELT API endpoint
search_term = "trump"
API_URL = f"https://api.gdeltproject.org/api/v2/doc/doc?query={search_term}&mode=ArtList&maxrecords=50&format=json"

# Custom headers to mimic a browser
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
}

def fetch_articles():
    try:
        response = requests.get(API_URL, headers=headers)
        if response.status_code == 200:
            data = response.json()
            articles = data.get("articles", [])
            print(articles)
            return articles
        elif response.status_code == 429:
            print("Rate limit reached (429). Waiting before retrying...")
            return []  # Return empty list to trigger backoff
        else:
            print("Error fetching data:", response.status_code)
    except Exception as e:
        print("Exception during API request:", e)
    return []

def main():
    backoff = 60  # Start with a 60-second delay on rate-limit hits
    while True:
        articles = fetch_articles()
        if articles:
            backoff = 60  # reset backoff on success
            print(f"Fetched {len(articles)} articles.")
            for article in articles:
                producer.send('geopolitics_events', article)
            producer.flush()
        else:
            print("No articles fetched, possibly rate-limited. Backing off...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 3600)  # exponential backoff with an upper limit
            continue

        time.sleep(60)  # Normal polling interval

if __name__ == '__main__':
    main()
