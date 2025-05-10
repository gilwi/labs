from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaError, UnknownTopicOrPartitionError
import json
import time
import logging

KAFKA_HOST = 'kafka-node:9092'
TOPIC_NAME = 'payments'
GROUP_ID = 'payment-consumers'

logging.basicConfig(level=logging.INFO)

def wait_for_kafka():
    """Wait for Kafka broker to become available."""
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_HOST)
            logging.info("✅ Kafka is available.")
            return admin
        except NoBrokersAvailable:
            logging.warning("🕒 Waiting for Kafka broker...")
            time.sleep(3)

def wait_for_topic(admin):
    """Wait until the specified topic exists."""
    while True:
        try:
            topics = admin.list_topics()
            if TOPIC_NAME in topics:
                logging.info(f"✅ Topic '{TOPIC_NAME}' is available.")
                return
            logging.warning(f"⏳ Waiting for topic '{TOPIC_NAME}'...")
            time.sleep(3)
        except KafkaError as e:
            logging.warning(f"Kafka error: {e}")
            time.sleep(3)

def start_consumer():
    """Start the Kafka consumer."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_HOST,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=GROUP_ID,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logging.info("🚀 Consumer started and listening for messages...")
            for message in consumer:
                logging.info(f"📥 Received: {message.value}")
        except UnknownTopicOrPartitionError:
            logging.warning("❗ Topic not ready yet. Retrying...")
            time.sleep(3)
        except KafkaError as e:
            logging.error(f"Kafka error: {e}")
            time.sleep(3)

if __name__ == "__main__":
    admin = wait_for_kafka()
    wait_for_topic(admin)
    start_consumer()
