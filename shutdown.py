#!/usr/bin/env python3
import os
import subprocess
import logging
import time
import signal
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='shutdown.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

# Add console handler to see logs in terminal
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

def stop_process_by_name(process_name):
    """Stop processes by name using pkill"""
    logger.info(f"Attempting to stop {process_name} processes...")
    try:
        subprocess.run(["pkill", "-f", process_name], stderr=subprocess.PIPE)
        logger.info(f"Stopped {process_name} processes")
        return True
    except Exception as e:
        logger.error(f"Error stopping {process_name} processes: {e}")
        return False

def stop_docker_containers():
    """Stop Docker containers using docker-compose"""
    logger.info("Stopping Docker containers...")
    
    # Find docker-compose.yml
    compose_paths = [
        "docker-compose.yml",
        "docker/docker-compose.yml"
    ]
    
    compose_file = None
    for path in compose_paths:
        if os.path.exists(path):
            compose_file = path
            break
    
    if not compose_file:
        logger.error("Could not find docker-compose.yml")
        return False
    
    try:
        # Stop containers
        logger.info("Stopping containers...")
        subprocess.run(
            ["docker-compose", "-f", compose_file, "down", "--volumes", "--remove-orphans"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Verify containers are stopped
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        running_containers = result.stdout.strip().split('\n')
        target_containers = ["kafka", "zookeeper", "scylla"]
        
        still_running = [c for c in target_containers if any(c in container for container in running_containers)]
        if still_running:
            logger.warning(f"Some containers are still running: {still_running}")
            return False
            
        logger.info("All containers stopped successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error stopping Docker containers: {e}")
        return False

def cleanup_files():
    """Clean up log files and checkpoints"""
    logger.info("Cleaning up files...")
    try:
        # Clean up log files
        log_files = ['producer.log', 'spark.log', 'startup.log']
        for log_file in log_files:
            if os.path.exists(log_file):
                os.remove(log_file)
                logger.info(f"Removed {log_file}")
        
        # Clean up Spark checkpoint directory
        checkpoint_dir = os.path.join(os.getcwd(), "checkpoint")
        if os.path.exists(checkpoint_dir):
            import shutil
            shutil.rmtree(checkpoint_dir)
            logger.info("Removed Spark checkpoint directory")
            
        return True
    except Exception as e:
        logger.error(f"Error cleaning up files: {e}")
        return False

def main():
    """Main function to stop all components"""
    logger.info("Starting shutdown sequence...")
    
    # Stop Python processes
    processes_to_stop = [
        "kafka_producer.py",
        "spark_streaming.py"
    ]
    
    for process in processes_to_stop:
        stop_process_by_name(process)
    
    # Stop Docker containers
    if not stop_docker_containers():
        logger.warning("Some containers may still be running")
    
    # Clean up files
    if not cleanup_files():
        logger.warning("Some files could not be cleaned up")
    
    logger.info("Shutdown completed")

if __name__ == "__main__":
    main() 