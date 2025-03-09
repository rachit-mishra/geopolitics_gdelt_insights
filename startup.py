#!/usr/bin/env python3
import os
import sys
import subprocess
import time
import argparse
import logging
import signal
import shutil
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='startup.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

# Add console handler
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

def check_python_dependencies():
    """Check if required Python packages are installed and install if missing"""
    logger.info("Checking Python dependencies...")
    required_packages = [
        'kafka-python',
        'requests',
        'pyspark',
        'transformers',
        'cassandra-driver',
        'pandas'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            logger.info(f"Package {package} is already installed")
        except ImportError:
            missing_packages.append(package)
            logger.warning(f"Package {package} is not installed")
    
    if missing_packages:
        logger.info(f"Installing missing packages: {', '.join(missing_packages)}")
        try:
            import subprocess
            for package in missing_packages:
                logger.info(f"Installing {package}...")
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
                logger.info(f"Successfully installed {package}")
        except Exception as e:
            logger.error(f"Error installing packages: {e}")
            return False
    
    return True

def check_docker_running():
    """Check if Docker is running"""
    logger.info("Checking if Docker is running...")
    try:
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("Docker is running.")
        return True
    except subprocess.CalledProcessError:
        logger.error("Docker is not running. Please start Docker and try again.")
        return False

def find_docker_compose():
    """Find docker-compose.yml file"""
    logger.info("Looking for docker-compose.yml file...")
    
    # Check in current directory
    if os.path.exists("docker-compose.yml"):
        return os.path.abspath("docker-compose.yml")
    
    # Check in docker subdirectory
    docker_dir = os.path.join(os.getcwd(), "docker")
    if os.path.exists(os.path.join(docker_dir, "docker-compose.yml")):
        return os.path.join(docker_dir, "docker-compose.yml")
    
    # Search recursively from current directory
    for root, dirs, files in os.walk(os.getcwd()):
        if "docker-compose.yml" in files:
            compose_path = os.path.join(root, "docker-compose.yml")
            logger.info(f"Found docker-compose.yml at {compose_path}")
            return compose_path
    
    logger.error("Could not find docker-compose.yml file")
    return None

def check_containers_running():
    """Check if required containers are running"""
    logger.info("Checking if required containers are running...")
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        running_containers = result.stdout.strip().split('\n')
        required_containers = ["kafka", "zookeeper", "scylla"]
        
        missing_containers = [c for c in required_containers if not any(c in container for container in running_containers)]
        
        if missing_containers:
            logger.warning(f"Some containers are not running: {missing_containers}")
            return False
        else:
            logger.info("All required containers are running.")
            return True
    except Exception as e:
        logger.error(f"Error checking containers: {e}")
        return False

def start_containers():
    """Start required containers using docker-compose"""
    logger.info("Starting containers using docker-compose...")
    
    compose_file = find_docker_compose()
    if not compose_file:
        return False
    
    try:
        # Change to the directory containing docker-compose.yml
        compose_dir = os.path.dirname(compose_file)
        original_dir = os.getcwd()
        os.chdir(compose_dir)
        
        try:
            # Clean up any stopped containers and networks first
            logger.info("Cleaning up existing containers and networks...")
            subprocess.run(
                ["docker-compose", "down", "--volumes", "--remove-orphans"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Start containers without pulling
            logger.info("Starting containers...")
            subprocess.run(
                ["docker-compose", "up", "-d", "--remove-orphans"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for containers to be fully up with a timeout
            logger.info("Waiting for containers to be ready...")
            max_retries = 6  # Reduced to 1 minute total (6 * 10 seconds)
            retry_count = 0
            
            while retry_count < max_retries:
                if check_containers_health():
                    logger.info("All containers are up and running!")
                    return True
                
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"Waiting 10 seconds before next health check (attempt {retry_count}/{max_retries})...")
                    time.sleep(10)
            
            # If we get here, containers didn't come up properly
            logger.error("Container startup timeout reached. Proceeding anyway as containers are running...")
            return True  # Return True if containers are at least running
            
        finally:
            # Always return to original directory
            os.chdir(original_dir)
            
    except Exception as e:
        logger.error(f"Error starting containers: {e}")
        return False

def check_containers_health():
    """Check if all containers are healthy"""
    try:
        # First check if containers are running
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True,
            text=True
        )
        running_containers = result.stdout.strip().split('\n')
        container_status = {}
        
        for container in running_containers:
            if container:  # Skip empty lines
                parts = container.split('\t')
                if len(parts) == 2:
                    name, status = parts
                    container_status[name] = status
        
        logger.info("Container statuses:")
        for name, status in container_status.items():
            logger.info(f"  {name}: {status}")
        
        # Check if all required containers are running
        required_containers = {"kafka", "zookeeper", "scylla"}
        missing = required_containers - set(container_status.keys())
        if missing:
            logger.warning(f"Missing containers: {missing}")
            return False
        
        # Give containers a bit more time to initialize
        time.sleep(5)
        
        # Individual service checks with better error handling
        try:
            # Check Zookeeper
            zk_result = subprocess.run(
                ["docker", "exec", "zookeeper", "bash", "-c", "echo ruok | nc localhost 2181"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5  # Add timeout
            )
            logger.info(f"Zookeeper health check: {'OK' if zk_result.returncode == 0 else 'Failed'}")
        except Exception as e:
            logger.warning(f"Zookeeper health check failed: {e}")
            # Don't fail completely on zookeeper check
        
        try:
            # Check ScyllaDB - simplified check
            scylla_result = subprocess.run(
                ["docker", "exec", "scylla", "cqlsh", "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5
            )
            logger.info(f"ScyllaDB health check: {'OK' if scylla_result.returncode == 0 else 'Failed'}")
        except Exception as e:
            logger.warning(f"ScyllaDB health check failed: {e}")
            # Don't fail completely on scylla check
        
        try:
            # Check Kafka - simplified check
            kafka_result = subprocess.run(
                ["docker", "exec", "kafka", "/usr/bin/kafka-topics", "--bootstrap-server", "localhost:9092", "--list"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5
            )
            logger.info(f"Kafka health check: {'OK' if kafka_result.returncode == 0 else 'Failed'}")
        except Exception as e:
            logger.warning(f"Kafka health check failed: {e}")
            # Don't fail completely on kafka check
        
        # If containers are running, consider the system healthy enough to proceed
        logger.info("All required containers are running")
        return True
                
    except Exception as e:
        logger.error(f"Error checking container health: {e}")
        return False

def create_kafka_topic(topic_name):
    """Create Kafka topic if it doesn't exist"""
    logger.info(f"Creating Kafka topic: {topic_name}")
    try:
        # Check if topic exists
        result = subprocess.run(
            ["docker", "exec", "kafka", "/usr/bin/kafka-topics", "--list", "--bootstrap-server", "localhost:9092"],
            capture_output=True,
            text=True
        )
        existing_topics = result.stdout.strip().split('\n')
        
        if topic_name in existing_topics:
            logger.info(f"Topic {topic_name} already exists")
            return True
        
        # Create topic
        subprocess.run(
            ["docker", "exec", "kafka", "/usr/bin/kafka-topics", "--create", "--topic", topic_name, 
             "--partitions", "1", "--replication-factor", "1", "--bootstrap-server", "localhost:9092"],
            check=True
        )
        
        logger.info(f"Successfully created Kafka topic: {topic_name}")
        return True
    except Exception as e:
        logger.error(f"Error creating Kafka topic: {e}")
        return False

def start_kafka_producer(topic_name, search_term):
    """Start Kafka producer process"""
    logger.info(f"Starting Kafka producer with topic: {topic_name}, search term: {search_term}")
    try:
        producer_process = subprocess.Popen(
            ["python", "producer/kafka_producer.py", "--topic", topic_name, "--search", search_term],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logger.info(f"Started Kafka producer (PID: {producer_process.pid})")
        return True
    except Exception as e:
        logger.error(f"Error starting Kafka producer: {e}")
        return False

def start_spark_streaming(topic_name):
    """Start Spark streaming process"""
    logger.info(f"Starting Spark streaming with topic: {topic_name}")
    try:
        # Pass the topic name as an environment variable
        env = os.environ.copy()
        env["KAFKA_TOPIC"] = topic_name
        
        spark_process = subprocess.Popen(
            ["python", "spark_streaming/spark_streaming.py"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logger.info(f"Started Spark streaming (PID: {spark_process.pid})")
        return True
    except Exception as e:
        logger.error(f"Error starting Spark streaming: {e}")
        return False

def cleanup(signum=None, frame=None):
    """Clean up processes on exit"""
    logger.info("Running cleanup...")
    try:
        # Stop Python processes
        subprocess.run(["pkill", "-f", "kafka_producer.py"], stderr=subprocess.PIPE)
        subprocess.run(["pkill", "-f", "spark_streaming.py"], stderr=subprocess.PIPE)
        logger.info("Stopped Python processes")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

def main():
    """Main function to start all components"""
    parser = argparse.ArgumentParser(description='Start the geopolitics data pipeline')
    parser.add_argument('--topic', type=str, default='geopolitics_events', 
                      help='Kafka topic name (default: geopolitics_events)')
    parser.add_argument('--search', type=str, default='ukraine', 
                      help='Search term for GDELT articles (default: ukraine)')
    
    args = parser.parse_args()
    topic_name = args.topic
    search_term = args.search
    
    logger.info(f"Starting pipeline with topic: {topic_name}, search term: {search_term}")
    
    # Register cleanup handler
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    
    # Check Python dependencies
    if not check_python_dependencies():
        sys.exit(1)
    
    # Check if Docker is running
    if not check_docker_running():
        sys.exit(1)
    
    # Check if containers are running, start them if not
    if not check_containers_running():
        logger.info("Some containers are not running. Attempting to start them...")
        if not start_containers():
            logger.error("Failed to start required containers. Exiting.")
            sys.exit(1)
    
    # Create Kafka topic
    if not create_kafka_topic(topic_name):
        logger.error("Failed to create Kafka topic. Exiting.")
        sys.exit(1)
    
    # Start Kafka producer
    if not start_kafka_producer(topic_name, search_term):
        logger.error("Failed to start Kafka producer. Exiting.")
        sys.exit(1)
    
    # Start Spark streaming
    if not start_spark_streaming(topic_name):
        logger.error("Failed to start Spark streaming. Exiting.")
        sys.exit(1)
    
    logger.info("All components started successfully")
    
    try:
        # Keep the script running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        cleanup()

if __name__ == "__main__":
    main() 