from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from faker import Faker
import json
import time
import random
import logging

KAFKA_HOST = 'kafka-node:9092'
TOPIC_NAME = 'payments'

fake = Faker()
logging.basicConfig(level=logging.INFO)

# Wait for Kafka to become available
def wait_for_kafka():
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_HOST)
            logging.info("✅ Kafka is available")
            return admin
        except NoBrokersAvailable:
            logging.warning("🕒 Waiting for Kafka broker...")
            time.sleep(3)

# Create topic if it doesn't exist
def create_topic(admin):
    topic = NewTopic(name=TOPIC_NAME)
    try:
        admin.create_topics([topic])
        logging.info(f"✅ Topic '{TOPIC_NAME}' created")
    except TopicAlreadyExistsError:
        logging.info(f"ℹ️ Topic '{TOPIC_NAME}' already exists")

# Generate fake payment data
def generate_payment():
    return {
        'transaction_id': fake.uuid4(),
        'timestamp': fake.iso8601(),
        'payer_name': fake.name(),
        'payer_email': fake.email(),
        'amount': round(random.uniform(10.0, 1000.0), 2),
        'currency': random.choice(['USD', 'EUR', 'GBP']),
        'payment_method': random.choice(['credit_card', 'paypal', 'bank_transfer']),
        'status': random.choice(['pending', 'completed', 'failed'])
    }

if __name__ == "__main__":
    admin_client = wait_for_kafka()
    create_topic(admin_client)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_HOST,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    logging.info(f"🚀 Starting producer for topic '{TOPIC_NAME}'")

    while True:
        payment = generate_payment()
        producer.send(TOPIC_NAME, payment)
        logging.info(f"📤 Sent: {payment}")
        time.sleep(1)
