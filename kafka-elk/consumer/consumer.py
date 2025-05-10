import time
import logging
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC = "payments"
BOOTSTRAP_SERVERS = ["kafka-node:9092"]
GROUP_ID = "payment-consumers"
RETRY_DELAY = 5  # seconds

def wait_for_kafka():
    """Wait until Kafka is reachable and topic is available."""
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
            )

            # Force metadata refresh
            logger.info("🔄 Refreshing metadata...")
            topics = consumer.topics()
            if TOPIC not in topics:
                logger.warning(f"❌ Topic '{TOPIC}' not found. Retrying...")
                consumer.close()
                time.sleep(RETRY_DELAY)
                continue

            logger.info(f"✅ Connected to Kafka. Topic '{TOPIC}' is available.")
            return consumer

        except KafkaError as e:
            logger.error(f"❗ Kafka not ready: {e}. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

def consume_messages(consumer):
    """Poll messages from the topic and print them."""
    logger.info("🚀 Consumer started and listening for messages...")

    # Optional: directly assign to partition 0 (remove if you want group balancing)
    # consumer.assign([TopicPartition(TOPIC, 0)])

    try:
        while True:
            logger.info("⏳ Polling Kafka...")
            msg_pack = consumer.poll(timeout_ms=1000)
            for tp, messages in msg_pack.items():
                for msg in messages:
                    logger.info(f"📥 Received: {msg.value.decode('utf-8')}")
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("👋 Stopping consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consumer = wait_for_kafka()
    consume_messages(consumer)
