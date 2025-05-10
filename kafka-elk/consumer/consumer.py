import json
import logging
import os
import socket
import time
import uuid
from datetime import datetime

import requests
from kafka import KafkaConsumer
from kafka import errors as kafka_errors
from kafka.admin import KafkaAdminClient
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka_consumer")

# Configurable settings - Kafka
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka-node")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9092"))
TOPIC_NAME = os.getenv("TOPIC_NAME", "payments")
GROUP_ID = os.getenv("GROUP_ID", "payments-group")
CONSUMER_ID = os.uname()[1]
# CONSUMER_ID = os.getenv("CONSUMER_ID", "unknown")

# Configurable settings - Elasticsearch
ES_HOST = os.getenv("ES_HOST", "elastic-node")
ES_PORT = int(os.getenv("ES_PORT", "9200"))
# Fixed index name instead of environment variable
FIXED_INDEX = "all-kafka-data"
ES_USERNAME = os.getenv("ES_USERNAME", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "changeme")
ES_USE_SSL = os.getenv("ES_USE_SSL", "true").lower() in ("true", "yes", "1")
ES_VERIFY_CERTS = os.getenv("ES_VERIFY_CERTS", "false").lower() in ("true", "yes", "1")


def wait_for_kafka(host, port, timeout=60):
    """Wait for Kafka broker to become available."""
    logger.info(f"[{CONSUMER_ID}] Waiting for Kafka broker at {host}:{port}...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                logger.info(f"[{CONSUMER_ID}] Kafka broker is available.")
                return True
        except OSError as e:
            logger.debug(f"[{CONSUMER_ID}] Kafka not yet available: {str(e)}")
            time.sleep(2)
    logger.error(
        f"[{CONSUMER_ID}] Could not connect to Kafka broker at {host}:{port} within {timeout} seconds"
    )
    return False


def wait_for_topic(topic, bootstrap_servers, timeout=60):
    """Wait for Kafka topic to become available."""
    logger.info(f"[{CONSUMER_ID}] Waiting for topic '{topic}' to become available...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin = None
            try:
                admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
                topics = admin.list_topics()
                if topic in topics:
                    logger.info(f"[{CONSUMER_ID}] Topic '{topic}' is available.")
                    return True
                logger.debug(
                    f"[{CONSUMER_ID}] Topic '{topic}' not found yet, available topics: {topics}"
                )
            finally:
                if admin:
                    try:
                        admin.close()
                    except Exception as e:
                        logger.warning(
                            f"[{CONSUMER_ID}] Error closing admin client: {str(e)}"
                        )
            time.sleep(2)
        except kafka_errors.NoBrokersAvailable as e:
            logger.warning(f"[{CONSUMER_ID}] No brokers available: {str(e)}")
            time.sleep(2)
        except Exception as e:
            logger.warning(f"[{CONSUMER_ID}] Error checking for topic: {str(e)}")
            time.sleep(2)
    logger.error(f"[{CONSUMER_ID}] Topic '{topic}' not found within {timeout} seconds")
    return False


def wait_for_elasticsearch(
    host, port, username, password, use_ssl=True, verify_certs=False, timeout=60
):
    """Wait for Elasticsearch to become available."""
    logger.info(f"[{CONSUMER_ID}] Waiting for Elasticsearch at {host}:{port}...")
    deadline = time.time() + timeout
    protocol = "https" if use_ssl else "http"
    url = f"{protocol}://{host}:{port}"
    while time.time() < deadline:
        try:
            response = requests.get(
                url,
                auth=HTTPBasicAuth(username, password),
                verify=verify_certs,
                timeout=5,
            )
            if response.status_code == 200:
                logger.info(f"[{CONSUMER_ID}] Elasticsearch is available.")
                cluster_info = response.json()
                logger.info(
                    f"[{CONSUMER_ID}] Connected to Elasticsearch cluster: {cluster_info.get('cluster_name', 'unknown')}"
                )
                return True
            logger.warning(
                f"[{CONSUMER_ID}] Elasticsearch returned status code {response.status_code}"
            )
        except requests.exceptions.RequestException as e:
            logger.debug(f"[{CONSUMER_ID}] Elasticsearch not yet available: {str(e)}")
        time.sleep(2)
    logger.error(
        f"[{CONSUMER_ID}] Could not connect to Elasticsearch at {url} within {timeout} seconds"
    )
    return False


def ensure_es_index(
    host, port, index_name, username, password, use_ssl=True, verify_certs=False
):
    """Ensure the Elasticsearch index exists."""
    protocol = "https" if use_ssl else "http"
    url = f"{protocol}://{host}:{port}/{index_name}"
    try:
        # Check if index exists
        response = requests.head(
            url, auth=HTTPBasicAuth(username, password), verify=verify_certs
        )
        # If index doesn't exist, create it
        if response.status_code == 404:
            logger.info(
                f"[{CONSUMER_ID}] Creating Elasticsearch index '{index_name}'..."
            )
            # Basic index settings
            index_settings = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 1},
                "mappings": {
                    "properties": {
                        "kafka_topic": {"type": "keyword"},
                        "kafka_partition": {"type": "integer"},
                        "kafka_offset": {"type": "long"},
                        "kafka_timestamp": {"type": "date"},
                        "ingest_timestamp": {"type": "date"},
                        "message": {"type": "text"},
                        "message_json": {"type": "object", "enabled": True},
                    }
                },
            }
            create_response = requests.put(
                url,
                auth=HTTPBasicAuth(username, password),
                json=index_settings,
                verify=verify_certs,
                headers={"Content-Type": "application/json"},
            )
            if create_response.status_code in [200, 201]:
                logger.info(
                    f"[{CONSUMER_ID}] Successfully created Elasticsearch index '{index_name}'"
                )
                return True
            else:
                if create_response.status_code == 400 and 'resource_already_exists_exception' in create_response.text:
                    logger.info(
                        f"[{CONSUMER_ID}] Response code: {create_response.status_code} because index was created while preparing the request (resource_already_exists_exception)."
                    )
                    return True
                logger.error(
                    f"[{CONSUMER_ID}] Failed to create Elasticsearch index: {create_response.text}"
                )
                return False
        elif response.status_code == 200:
            logger.info(
                f"[{CONSUMER_ID}] Elasticsearch index '{index_name}' already exists"
            )
            return True
        else:
            logger.warning(
                f"[{CONSUMER_ID}] Unexpected response when checking index: {response.status_code}"
            )
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"[{CONSUMER_ID}] Error ensuring Elasticsearch index: {str(e)}")
        return False


def index_to_elasticsearch(
    message,
    host,
    port,
    index_name,
    username,
    password,
    use_ssl=True,
    verify_certs=False,
):
    """Index a message to Elasticsearch."""
    protocol = "https" if use_ssl else "http"
    # Generate a unique ID for the document
    doc_id = str(uuid.uuid4())
    url = f"{protocol}://{host}:{port}/{index_name}/_doc/{doc_id}"

    # Prepare the document
    document = {
        "kafka_topic": message.topic,
        "kafka_partition": message.partition,
        "kafka_offset": message.offset,
        "kafka_timestamp": (
            datetime.fromtimestamp(message.timestamp / 1000).isoformat()
            if message.timestamp
            else None
        ),
        "ingest_timestamp": datetime.utcnow().isoformat(),
        "message": message.value,
    }

    # Try to parse the message as JSON
    try:
        message_json = json.loads(message.value)
        document["message_json"] = message_json
    except (json.JSONDecodeError, TypeError):
        # If not JSON, just use the raw message
        pass

    try:
        response = requests.put(
            url,
            auth=HTTPBasicAuth(username, password),
            json=document,
            verify=verify_certs,
            headers={"Content-Type": "application/json"},
        )
        if response.status_code in [200, 201]:
            logger.debug(
                f"[{CONSUMER_ID}] Successfully indexed message to Elasticsearch: {doc_id}"
            )
            return True
        else:
            logger.error(
                f"[{CONSUMER_ID}] Failed to index message to Elasticsearch: {response.text}"
            )
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"[{CONSUMER_ID}] Error indexing to Elasticsearch: {str(e)}")
        return False


def main():
    # Log startup information
    logger.info(
        f"[{CONSUMER_ID}] Starting Kafka consumer with Elasticsearch integration"
    )
    logger.info(
        f"[{CONSUMER_ID}] Kafka config: KAFKA_HOST={KAFKA_HOST}, KAFKA_PORT={KAFKA_PORT}"
    )
    logger.info(f"[{CONSUMER_ID}] Kafka topic: {TOPIC_NAME}, GROUP_ID: {GROUP_ID}")
    logger.info(
        f"[{CONSUMER_ID}] Elasticsearch config: ES_HOST={ES_HOST}, ES_PORT={ES_PORT}, FIXED_INDEX={FIXED_INDEX}"
    )
    logger.info(
        f"[{CONSUMER_ID}] Elasticsearch security: SSL={ES_USE_SSL}, Verify Certs={ES_VERIFY_CERTS}"
    )

    bootstrap_servers = [f"{KAFKA_HOST}:{KAFKA_PORT}"]

    # Ensure Kafka is available
    if not wait_for_kafka(KAFKA_HOST, KAFKA_PORT):
        logger.error(f"[{CONSUMER_ID}] Failed to connect to Kafka. Exiting.")
        return

    # Ensure topic exists
    if not wait_for_topic(TOPIC_NAME, bootstrap_servers):
        logger.error(f"[{CONSUMER_ID}] Failed to find topic. Exiting.")
        return

    # Ensure Elasticsearch is available
    if not wait_for_elasticsearch(
        ES_HOST, ES_PORT, ES_USERNAME, ES_PASSWORD, ES_USE_SSL, ES_VERIFY_CERTS
    ):
        logger.error(f"[{CONSUMER_ID}] Failed to connect to Elasticsearch. Exiting.")
        return

    # Ensure index exists - using the fixed index name
    if not ensure_es_index(
        ES_HOST,
        ES_PORT,
        FIXED_INDEX,  # Use fixed index name
        ES_USERNAME,
        ES_PASSWORD,
        ES_USE_SSL,
        ES_VERIFY_CERTS,
    ):
        logger.error(f"[{CONSUMER_ID}] Failed to ensure Elasticsearch index. Exiting.")
        return

    consumer = None
    try:
        logger.info(f"[{CONSUMER_ID}] Creating consumer for topic '{TOPIC_NAME}'")
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=GROUP_ID,
            client_id=f"{GROUP_ID}-{CONSUMER_ID}",
            value_deserializer=lambda x: x.decode("utf-8") if x else None,
            consumer_timeout_ms=10000,  # 10 seconds timeout
        )

        logger.info(
            f"[{CONSUMER_ID}] Consumer started with GROUP_ID: {GROUP_ID}. Waiting for messages..."
        )

        # Display assignment information when partitions are assigned
        for partition in consumer.assignment():
            logger.info(f"[{CONSUMER_ID}] Assigned to partition: {partition.partition}")

        try:
            message_count = 0
            indexed_count = 0
            failed_count = 0

            while True:
                # Use poll instead of iterator to have more control
                messages = consumer.poll(timeout_ms=1000, max_records=10)

                if not messages:
                    logger.debug(f"[{CONSUMER_ID}] No messages received during polling")
                    continue

                for tp, records in messages.items():
                    for record in records:
                        message_count += 1
                        logger.info(
                            f"[{CONSUMER_ID} - Partition {record.partition} - Offset {record.offset}] {record.value}"
                        )

                        # Index message to Elasticsearch using the fixed index
                        if index_to_elasticsearch(
                            record,
                            ES_HOST,
                            ES_PORT,
                            FIXED_INDEX,  # Use fixed index name
                            ES_USERNAME,
                            ES_PASSWORD,
                            ES_USE_SSL,
                            ES_VERIFY_CERTS,
                        ):
                            indexed_count += 1
                        else:
                            failed_count += 1

                # Commit offsets explicitly
                consumer.commit()

                # Log statistics periodically
                if message_count % 100 == 0 or (message_count > 0 and not messages):
                    logger.info(
                        f"[{CONSUMER_ID}] Stats: Messages processed: {message_count}, "
                        f"Indexed: {indexed_count}, Failed: {failed_count}"
                    )

                logger.debug(
                    f"[{CONSUMER_ID}] Committed offsets after processing batch"
                )

        except KeyboardInterrupt:
            logger.info(f"[{CONSUMER_ID}] Received shutdown signal")

    except kafka_errors.NoBrokersAvailable as e:
        logger.error(f"[{CONSUMER_ID}] No Kafka brokers available: {str(e)}")
    except Exception as e:
        logger.error(
            f"[{CONSUMER_ID}] Error during consumer operation: {str(e)}", exc_info=True
        )
    finally:
        try:
            if consumer:
                logger.info(f"[{CONSUMER_ID}] Closing consumer")
                consumer.close(autocommit=True)
        except Exception as e:
            logger.error(f"[{CONSUMER_ID}] Error closing consumer: {str(e)}")

        # Final statistics
        logger.info(
            f"[{CONSUMER_ID}] Final stats: Messages processed: {message_count}, "
            f"Indexed: {indexed_count}, Failed: {failed_count}"
        )


if __name__ == "__main__":
    main()
