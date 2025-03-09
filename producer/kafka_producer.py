import requests
import json
import time
import logging
from kafka import KafkaProducer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='producer.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

# Add console handler
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

class GDELTProducer:
    def __init__(self, topic_name, search_term):
        self.topic_name = topic_name
        self.search_term = search_term
        self.producer = None
        self.connect_to_kafka()
    
    def connect_to_kafka(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info("Successfully connected to Kafka broker")
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {str(e)}")
            raise
    
    def send_to_kafka(self, article):
        try:
            future = self.producer.send(self.topic_name, article)
            future.get(timeout=10)  # Wait for message to be delivered
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {str(e)}")
    
    def close(self):
        if self.producer:
            logger.info(f"Closing the Kafka producer with {self.producer.config['request.timeout.ms'] / 1000} secs timeout.")
            self.producer.close()

    def fetch_articles(self):
        initial_backoff = 5  # Start with 5 seconds
        max_backoff = 60    # Max backoff of 1 minute
        max_retries = 5
        retries = 0
        
        while retries < max_retries:
            try:
                url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={self.search_term}&mode=artlist&format=json&maxrecords=25"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (compatible; GDELTExplorer/1.0; +http://example.org/bot)',
                    'Accept': 'application/json'
                }
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    logger.info(f"Successfully fetched {self.search_term} articles from GDELT")
                    return response.json()
                elif response.status_code == 429:
                    backoff_time = min(initial_backoff * (2 ** retries), max_backoff)
                    logger.warning(f"Rate limited. Waiting for {backoff_time} seconds before retry {retries + 1}/{max_retries}")
                    time.sleep(backoff_time)
                    retries += 1
                else:
                    logger.error(f"Error fetching articles: {response.status_code} - {response.text}")
                    return None
                    
            except Exception as e:
                logger.error(f"Error fetching articles: {str(e)}")
                return None
        
        logger.error("Max retries reached while fetching articles")
        return None

    def run(self):
        while True:
            try:
                logger.info(f"Fetching articles for search term: {self.search_term}")
                articles = self.fetch_articles()
                
                if articles and 'articles' in articles:
                    num_articles = len(articles['articles'])
                    logger.info(f"Found {num_articles} articles")
                    
                    for article in articles['articles']:
                        self.send_to_kafka(article)
                    
                    logger.info(f"Successfully sent {num_articles} articles to Kafka")
                else:
                    logger.warning(f"No articles found for search term: {self.search_term}")
                
                # Reduced sleep time between successful fetches
                sleep_time = 30  # 30 seconds instead of 600
                logger.info(f"Sleeping for {sleep_time} seconds before next fetch")
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in producer loop: {str(e)}")
                time.sleep(30)  # Wait 30 seconds before retrying on error

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='GDELT Article Producer')
    parser.add_argument('--topic', type=str, required=True, help='Kafka topic name')
    parser.add_argument('--search', type=str, required=True, help='Search term for GDELT articles')
    
    args = parser.parse_args()
    
    producer = GDELTProducer(args.topic, args.search)
    try:
        producer.run()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
        producer.close()
    except Exception as e:
        logger.error(f"Producer failed: {str(e)}")
        producer.close()
        raise