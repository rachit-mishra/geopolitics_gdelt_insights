from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, ConsistencyLevel
from collections import Counter
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
import numpy as np

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analytics.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GeopoliticsAnalytics:
    def __init__(self):
        try:
            auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
            self.cluster = Cluster(['localhost'], auth_provider=auth_provider, port=9042)
            
            # First connect without keyspace to create it if needed
            session = self.cluster.connect()
            
            # Create keyspace if not exists
            logger.info("Creating keyspace if not exists...")
            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS geopolitics
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                AND durable_writes = true
            """)
            
            # Now connect with the keyspace
            self.session = self.cluster.connect('geopolitics')
            
            # Verify table exists
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    url text,
                    processed_at timestamp,
                    title text,
                    seendate text,
                    socialimage text,
                    domain text,
                    language text,
                    sourcecountry text,
                    sentiment text,
                    PRIMARY KEY (url, processed_at)
                ) WITH CLUSTERING ORDER BY (processed_at DESC)
            """)
            
            logger.info("Successfully connected to ScyllaDB and verified schema")
        except Exception as e:
            logger.error(f"Error connecting to ScyllaDB: {e}", exc_info=True)
            raise

    def get_recent_articles(self, hours=24):
        try:
            # Get articles from the last 24 hours
            current_time = datetime.now()
            start_time = current_time - timedelta(hours=hours)
            
            # Modified query to work with ScyllaDB's requirements
            query = """
                SELECT * FROM geopolitics.articles 
                LIMIT 1000
            """
            logger.info(f"Executing query for articles in the last {hours} hours")
            
            # Execute with retry logic
            max_retries = 3
            retry_count = 0
            rows = None
            
            while retry_count < max_retries:
                try:
                    statement = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_ONE)
                    rows = self.session.execute(statement)
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        raise
                    logger.warning(f"Query attempt {retry_count} failed: {e}, retrying...")
                    time.sleep(2)
            
            if not rows:
                logger.warning("Query returned no results")
                return []
            
            articles = []
            row_count = 0
            
            # Convert rows to list and sort by processed_at in memory
            all_rows = list(rows)
            # Filter by time in memory since ScyllaDB doesn't support filtering on clustering key
            filtered_rows = [row for row in all_rows if row.processed_at >= start_time]
            sorted_rows = sorted(filtered_rows, key=lambda x: x.processed_at if hasattr(x, 'processed_at') else '', reverse=True)
            
            for row in sorted_rows:
                try:
                    article = {
                        'url': str(row.url) if hasattr(row, 'url') else '',
                        'title': str(row.title) if hasattr(row, 'title') else '',
                        'seendate': str(row.seendate) if hasattr(row, 'seendate') else '',
                        'domain': str(row.domain) if hasattr(row, 'domain') else '',
                        'language': str(row.language) if hasattr(row, 'language') else '',
                        'sourcecountry': str(row.sourcecountry) if hasattr(row, 'sourcecountry') else '',
                        'sentiment': str(row.sentiment) if hasattr(row, 'sentiment') else '',
                        'socialimage': str(row.socialimage) if hasattr(row, 'socialimage') else '',
                        'processed_at': row.processed_at if hasattr(row, 'processed_at') else None
                    }
                    articles.append(article)
                    row_count += 1
                    if row_count % 100 == 0:
                        logger.info(f"Processed {row_count} rows...")
                except Exception as e:
                    logger.error(f"Error processing row: {e}")
                    continue
            
            logger.info(f"Retrieved {len(articles)} articles from the last {hours} hours")
            return articles
            
        except Exception as e:
            logger.error(f"Error in get_recent_articles: {e}", exc_info=True)
            return []

    def analyze_data(self):
        try:
            articles = self.get_recent_articles(hours=24)
            
            if not articles:
                logger.warning("No articles retrieved for analysis")
                return
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(articles)
            
            # Data Quality Checks
            logger.info("\nData Quality Report:")
            total_records = len(df)
            null_counts = df.isnull().sum()
            empty_counts = df.apply(lambda x: (x == '').sum())
            
            logger.info(f"Total Records: {total_records}")
            logger.info("\nNull Values by Column:")
            logger.info(null_counts)
            logger.info("\nEmpty Values by Column:")
            logger.info(empty_counts)
            
            # Sentiment Analysis Quality
            sentiment_quality = {
                'total': len(df),
                'unknown': len(df[df['sentiment'] == 'UNKNOWN']),
                'valid': len(df[df['sentiment'].isin(['POSITIVE', 'NEGATIVE', 'NEUTRAL'])])
            }
            sentiment_quality['unknown_pct'] = (sentiment_quality['unknown'] / sentiment_quality['total']) * 100
            
            logger.info("\nSentiment Analysis Quality:")
            logger.info(f"Total Articles: {sentiment_quality['total']}")
            logger.info(f"Valid Sentiments: {sentiment_quality['valid']} ({100 - sentiment_quality['unknown_pct']:.1f}%)")
            logger.info(f"Unknown Sentiments: {sentiment_quality['unknown']} ({sentiment_quality['unknown_pct']:.1f}%)")
            
            # Time-based Analysis
            df['processed_at'] = pd.to_datetime(df['processed_at'])
            df['hour'] = df['processed_at'].dt.hour
            
            hourly_counts = df.groupby('hour').size()
            logger.info("\nHourly Distribution:")
            logger.info(hourly_counts)
            
            # Sentiment Distribution
            sentiment_counts = df['sentiment'].value_counts()
            logger.info(f"\nSentiment Distribution:")
            logger.info(sentiment_counts)
            
            # Sentiment by Country
            sentiment_by_country = pd.crosstab(df['sourcecountry'], df['sentiment'])
            logger.info("\nSentiment Distribution by Country (Top 10):")
            logger.info(sentiment_by_country.head(10))
            
            # Language Analysis
            language_sentiment = pd.crosstab(df['language'], df['sentiment'])
            logger.info("\nSentiment Distribution by Language (Top 10):")
            logger.info(language_sentiment.head(10))
            
            # Domain Analysis
            domain_counts = df['domain'].value_counts().head(10)
            logger.info(f"\nTop 10 Domains:")
            logger.info(domain_counts)
            
            # Recent Articles Sample
            recent_articles = df.sort_values('processed_at', ascending=False).head(5)
            logger.info(f"\nMost Recent Articles:")
            for _, article in recent_articles.iterrows():
                logger.info(f"Title: {article['title']}")
                logger.info(f"Date: {article['processed_at']}")
                logger.info(f"Country: {article['sourcecountry']}")
                logger.info(f"Language: {article['language']}")
                logger.info(f"Sentiment: {article['sentiment']}")
                logger.info(f"Domain: {article['domain']}\n")
            
            # Calculate ingestion rate
            time_range = (df['processed_at'].max() - df['processed_at'].min()).total_seconds() / 3600
            ingestion_rate = len(df) / time_range if time_range > 0 else 0
            
            logger.info("\nIngestion Statistics:")
            logger.info(f"Time Range: {time_range:.2f} hours")
            logger.info(f"Average Ingestion Rate: {ingestion_rate:.2f} articles/hour")
            
            return {
                'data_quality': {
                    'total_records': total_records,
                    'null_counts': null_counts.to_dict(),
                    'empty_counts': empty_counts.to_dict()
                },
                'sentiment_quality': sentiment_quality,
                'sentiment_distribution': sentiment_counts.to_dict(),
                'hourly_distribution': hourly_counts.to_dict(),
                'sentiment_by_country': sentiment_by_country.to_dict(),
                'language_sentiment': language_sentiment.to_dict(),
                'top_domains': domain_counts.to_dict(),
                'ingestion_rate': ingestion_rate
            }
            
        except Exception as e:
            logger.error(f"Error in analyze_data: {e}", exc_info=True)
            return None

    def run_continuous_analysis(self, interval_seconds=10):
        try:
            while True:
                logger.info("\n" + "="*50)
                logger.info(f"Running analysis at {datetime.now()}")
                result = self.analyze_data()
                if result:
                    logger.info(f"Analysis completed successfully")
                else:
                    logger.warning("Analysis returned no results")
                logger.info("="*50 + "\n")
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Analysis stopped by user")
        finally:
            self.cleanup()

    def cleanup(self):
        try:
            if hasattr(self, 'session'):
                self.session.shutdown()
            if hasattr(self, 'cluster'):
                self.cluster.shutdown()
            logger.info("Cleaned up database connections")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def main():
    try:
        analytics = GeopoliticsAnalytics()
        analytics.run_continuous_analysis(interval_seconds=10)
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 