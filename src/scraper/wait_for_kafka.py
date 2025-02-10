from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaConnectionError
import time
import logging
import socket
import os

logging.basicConfig(level=logging.DEBUG)  # Enable debug logging

def is_port_open(host, port, timeout=5):
    """Check if a port is open."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        # Get IP address for the hostname
        ip_addr = socket.gethostbyname(host)
        logging.debug(f"Resolved {host} to IP: {ip_addr}")
        
        result = sock.connect_ex((ip_addr, port))
        sock.close()
        
        if result == 0:
            logging.debug(f"Successfully connected to {host}:{port}")
            return True
        else:
            logging.debug(f"Failed to connect to {host}:{port}, error code: {result}")
            return False
    except socket.gaierror as e:
        logging.debug(f"DNS resolution failed for {host}: {str(e)}")
        return False
    except socket.error as e:
        logging.debug(f"Socket error when connecting to {host}:{port}: {str(e)}")
        return False

def wait_for_kafka(max_retries=15, retry_delay=10):
    """
    Wait for Kafka to become available.
    """
    kafka_host = os.getenv('KAFKA_HOST', 'kafka')
    kafka_port = int(os.getenv('KAFKA_PORT', '9092'))
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', f"{kafka_host}:{kafka_port}")
    
    logging.info(f"Attempting to connect to Kafka at {kafka_bootstrap_servers}")
    
    for attempt in range(max_retries):
        try:
            # First check if port is open
            if not is_port_open(kafka_host, kafka_port):
                logging.debug(f"Port {kafka_port} is not open on {kafka_host}")
                raise ConnectionError(f"Port {kafka_port} is not open on {kafka_host}")

            logging.debug(f"Creating KafkaAdminClient with bootstrap_servers={kafka_bootstrap_servers}")
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_bootstrap_servers,
                client_id='kafka_checker',
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
                security_protocol='PLAINTEXT',
                reconnect_backoff_ms=1000,
                reconnect_backoff_max_ms=5000,
                retry_backoff_ms=1000
            )
            
            # Test the connection by listing topics
            logging.debug("Attempting to list topics...")
            topics = admin_client.list_topics()
            admin_client.close()
            logging.info(f"Successfully connected to Kafka! Available topics: {topics}")
            return
            
        except (NoBrokersAvailable, KafkaConnectionError, ConnectionError) as e:
            if attempt < max_retries - 1:
                logging.warning(f"Waiting for Kafka to be ready... ({attempt + 1}/{max_retries})")
                logging.debug(f"Connection error: {str(e)}")
                time.sleep(retry_delay)
            else:
                raise Exception(
                    f"Failed to connect to Kafka at {kafka_bootstrap_servers} "
                    f"after {max_retries} attempts. Last error: {str(e)}"
                ) from e