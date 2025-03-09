from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, ConsistencyLevel
from collections import Counter
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
import numpy as np
import argparse

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
    def __init__(self, batch_size=50, concurrent_ops=20):
        try:
            self.batch_size = batch_size
            self.concurrent_ops = concurrent_ops
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

    def get_performance_metrics(self):
        """Get ScyllaDB performance metrics"""
        try:
            metrics = {
                'write_latency': [],
                'read_latency': [],
                'batch_write_latency': [],
                'range_scan_latency': [],
                'concurrent_ops': [],
                'throughput': 0,
                'total_operations': 0,
                'storage': {},
                'config': {
                    'batch_size': self.batch_size,
                    'concurrent_ops': self.concurrent_ops
                }
            }
            
            # Measure write latency with prepared statement
            test_data = {
                'url': 'test_url',
                'processed_at': datetime.now(),
                'title': 'test_title',
                'seendate': str(datetime.now()),
                'socialimage': 'test_image',
                'domain': 'test_domain',
                'language': 'en',
                'sourcecountry': 'US',
                'sentiment': 'POSITIVE'
            }
            
            # Single writes
            prepared = self.session.prepare("""
                INSERT INTO articles (url, processed_at, title, seendate, socialimage,
                                   domain, language, sourcecountry, sentiment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            
            logger.info("Testing single write performance...")
            for i in range(10):  # Test with 10 writes
                write_start = time.time()
                self.session.execute(prepared, (
                    f"test_url_{i}",
                    test_data['processed_at'],
                    test_data['title'],
                    test_data['seendate'],
                    test_data['socialimage'],
                    test_data['domain'],
                    test_data['language'],
                    test_data['sourcecountry'],
                    test_data['sentiment']
                ))
                metrics['write_latency'].append((time.time() - write_start) * 1000)
            
            # Batch writes
            logger.info(f"Testing batch write performance (batch size: {self.batch_size})...")
            batch_size = self.batch_size
            for batch_num in range(2):  # 2 batches
                batch_start = time.time()
                batch = []
                for i in range(batch_size):
                    batch.append((
                        f"batch_url_{batch_num}_{i}",
                        test_data['processed_at'],
                        test_data['title'],
                        test_data['seendate'],
                        test_data['socialimage'],
                        test_data['domain'],
                        test_data['language'],
                        test_data['sourcecountry'],
                        test_data['sentiment']
                    ))
                
                # Execute batch
                for params in batch:
                    self.session.execute(prepared, params)
                metrics['batch_write_latency'].append((time.time() - batch_start) * 1000 / batch_size)
            
            # Read patterns with different complexities
            logger.info("Testing read performance patterns...")
            read_patterns = [
                ("Simple Count", "SELECT COUNT(*) FROM articles"),
                ("Primary Key Lookup", "SELECT * FROM articles WHERE url = ? AND processed_at > ?"),
                ("Recent Records", "SELECT * FROM articles LIMIT 100"),
                ("Range Scan", "SELECT * FROM articles WHERE token(url) > ? LIMIT ?")
            ]
            
            for query_name, query in read_patterns:
                read_start = time.time()
                if "WHERE url = ?" in query:
                    prepared_read = self.session.prepare(query)
                    self.session.execute(prepared_read, (f"test_url_0", datetime.now() - timedelta(hours=1)))
                elif "token(url)" in query:
                    prepared_read = self.session.prepare(query)
                    self.session.execute(prepared_read, (0, 100))
                    metrics['range_scan_latency'].append((time.time() - read_start) * 1000)
                else:
                    self.session.execute(query)
                metrics['read_latency'].append((time.time() - read_start) * 1000)
            
            # Concurrent operations test
            logger.info(f"Testing concurrent operations performance ({self.concurrent_ops} operations)...")
            concurrent_start = time.time()
            futures = []
            for i in range(self.concurrent_ops):  # Test concurrent operations
                if i % 2 == 0:
                    # Write operation
                    futures.append(self.session.execute_async(prepared, (
                        f"concurrent_url_{i}",
                        test_data['processed_at'],
                        test_data['title'],
                        test_data['seendate'],
                        test_data['socialimage'],
                        test_data['domain'],
                        test_data['language'],
                        test_data['sourcecountry'],
                        test_data['sentiment']
                    )))
                else:
                    # Read operation
                    futures.append(self.session.execute_async("SELECT * FROM articles LIMIT 10"))
            
            # Wait for all operations to complete
            for future in futures:
                future.result()
            concurrent_time = (time.time() - concurrent_start) * 1000
            metrics['concurrent_ops'].append(concurrent_time / self.concurrent_ops)  # Average time per operation
            
            # Get table statistics and throughput
            metrics['total_operations'] = self.session.execute("SELECT COUNT(*) FROM articles").one()[0]
            
            # Calculate recent throughput
            hour_ago = datetime.now() - timedelta(hours=1)
            throughput_stmt = self.session.prepare(
                "SELECT COUNT(*) FROM articles WHERE processed_at > ? ALLOW FILTERING"
            )
            recent_ops = self.session.execute(throughput_stmt, (hour_ago,)).one()[0]
            metrics['throughput'] = recent_ops / 3600  # ops per second
            
            # Get table metrics
            keyspace_query = "SELECT * FROM system_schema.tables WHERE keyspace_name = 'geopolitics'"
            for table_data in self.session.execute(keyspace_query):
                # Get compaction and compression options safely
                compaction_options = getattr(table_data, 'compaction', {})
                compression_options = getattr(table_data, 'compression', {})
                
                # Extract compaction class name if available
                compaction_class = compaction_options.get('class', 'Unknown') if isinstance(compaction_options, dict) else str(compaction_options)
                if '.' in compaction_class:
                    compaction_class = compaction_class.split('.')[-1]  # Get just the class name
                
                metrics['storage'].update({
                    'table_name': table_data.table_name,
                    'gc_grace_seconds': table_data.gc_grace_seconds,
                    'bloom_filter_fp_chance': table_data.bloom_filter_fp_chance,
                    'compaction_strategy': compaction_class,
                    'compaction_options': compaction_options if isinstance(compaction_options, dict) else {},
                    'compression': compression_options if isinstance(compression_options, dict) else {}
                })
            
            return metrics
        except Exception as e:
            logger.error(f"Error getting performance metrics: {e}", exc_info=True)
            return None

    def analyze_performance(self):
        """Analyze and log ScyllaDB performance metrics"""
        try:
            metrics = self.get_performance_metrics()
            if not metrics:
                return
            
            logger.info("\nScyllaDB Performance Analysis:")
            logger.info("================================")
            logger.info(f"Configuration:")
            logger.info(f"  Batch Size: {metrics['config']['batch_size']}")
            logger.info(f"  Concurrent Operations: {metrics['config']['concurrent_ops']}")
            logger.info("================================")
            
            # Write Performance
            avg_write_latency = np.mean(metrics['write_latency'])
            p95_write = np.percentile(metrics['write_latency'], 95)
            p99_write = np.percentile(metrics['write_latency'], 99)
            
            logger.info("Single Write Performance:")
            logger.info(f"  Average Latency: {avg_write_latency:.2f}ms")
            logger.info(f"  P95 Latency: {p95_write:.2f}ms")
            logger.info(f"  P99 Latency: {p99_write:.2f}ms")
            
            # Batch Write Performance
            if metrics['batch_write_latency']:
                avg_batch = np.mean(metrics['batch_write_latency'])
                p95_batch = np.percentile(metrics['batch_write_latency'], 95)
                logger.info("\nBatch Write Performance (per record):")
                logger.info(f"  Average Latency: {avg_batch:.2f}ms")
                logger.info(f"  P95 Latency: {p95_batch:.2f}ms")
            
            # Read Performance
            avg_read_latency = np.mean(metrics['read_latency'])
            p95_read = np.percentile(metrics['read_latency'], 95)
            p99_read = np.percentile(metrics['read_latency'], 99)
            
            logger.info("\nRead Performance:")
            logger.info(f"  Average Latency: {avg_read_latency:.2f}ms")
            logger.info(f"  P95 Latency: {p95_read:.2f}ms")
            logger.info(f"  P99 Latency: {p99_read:.2f}ms")
            
            # Range Scan Performance
            if metrics['range_scan_latency']:
                avg_scan = np.mean(metrics['range_scan_latency'])
                logger.info("\nRange Scan Performance:")
                logger.info(f"  Average Latency: {avg_scan:.2f}ms")
            
            # Concurrent Operations
            if metrics['concurrent_ops']:
                avg_concurrent = np.mean(metrics['concurrent_ops'])
                logger.info("\nConcurrent Operations Performance:")
                logger.info(f"  Average Latency per Operation: {avg_concurrent:.2f}ms")
            
            # Throughput and Operations
            logger.info("\nOperations:")
            logger.info(f"  Total Records: {metrics['total_operations']:,}")
            logger.info(f"  Current Throughput: {metrics['throughput']:.2f} ops/second")
            
            # Storage Information
            if metrics['storage']:
                logger.info("\nStorage Configuration:")
                logger.info(f"  Table: {metrics['storage']['table_name']}")
                logger.info(f"  GC Grace Period: {metrics['storage']['gc_grace_seconds']} seconds")
                logger.info(f"  Bloom Filter FP Chance: {metrics['storage']['bloom_filter_fp_chance']}")
                
                # Log compaction details
                logger.info("\nCompaction Strategy:")
                logger.info(f"  Class: {metrics['storage']['compaction_strategy']}")
                for key, value in metrics['storage'].get('compaction_options', {}).items():
                    if key != 'class':
                        logger.info(f"  {key}: {value}")
                
                # Log compression details
                logger.info("\nCompression Settings:")
                for key, value in metrics['storage'].get('compression', {}).items():
                    logger.info(f"  {key}: {value}")
            
            return metrics
        except Exception as e:
            logger.error(f"Error analyzing performance: {e}")
            return None

    def run_continuous_analysis(self, interval_seconds=10, performance_mode=False):
        try:
            while True:
                logger.info("\n" + "="*50)
                logger.info(f"Running analysis at {datetime.now()}")
                
                if performance_mode:
                    # Run only performance analysis
                    perf_metrics = self.analyze_performance()
                    if perf_metrics:
                        logger.info("Performance analysis completed successfully")
                    else:
                        logger.warning("Performance analysis returned incomplete results")
                else:
                    # Run both regular and performance analysis
                    result = self.analyze_data()
                    perf_metrics = self.analyze_performance()
                    
                    if result and perf_metrics:
                        logger.info("Analysis completed successfully")
                    else:
                        logger.warning("Analysis returned incomplete results")
                
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
    parser = argparse.ArgumentParser(description='ScyllaDB Analytics')
    parser.add_argument('--performance-mode', action='store_true', help='Run in performance testing mode')
    parser.add_argument('--interval', type=int, default=5, help='Interval between performance checks in seconds')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for write operations')
    parser.add_argument('--concurrent-ops', type=int, default=20, help='Number of concurrent operations')
    parser.add_argument('--duration', type=int, default=60, help='Duration of performance test in seconds')
    parser.add_argument('--check-data', action='store_true', help='Check recent data in ScyllaDB')
    args = parser.parse_args()

    analytics = GeopoliticsAnalytics(batch_size=args.batch_size, concurrent_ops=args.concurrent_ops)

    if args.check_data:
        logger.info("Checking recent data in ScyllaDB...")
        articles = analytics.get_recent_articles(hours=1)  # Check last hour
        if articles:
            logger.info(f"Found {len(articles)} articles in the last hour")
            logger.info("\nMost recent articles:")
            for article in sorted(articles, key=lambda x: x['processed_at'], reverse=True)[:5]:
                logger.info(f"Title: {article['title']}")
                logger.info(f"Processed at: {article['processed_at']}")
                logger.info(f"Sentiment: {article['sentiment']}")
                logger.info("---")
        else:
            logger.warning("No articles found in the last hour")
        return

    if args.performance_mode:
        logger.info("Running in performance mode...")
        analytics.get_performance_metrics()
    else:
        analytics.analyze_data()

if __name__ == "__main__":
    main() 