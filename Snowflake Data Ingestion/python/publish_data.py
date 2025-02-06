import os
import logging
import sys
import confluent_kafka
from kafka.admin import KafkaAdminClient, NewTopic

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

kafka_brokers = os.getenv("REDPANDA_BROKERS")
topic_name = os.getenv("KAFKA_TOPIC")


def create_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_brokers, client_id='publish_data')
    topic_metadata = admin_client.list_topics()
    if topic_name not in topic_metadata:
        topic = NewTopic(name=topic_name, num_partitions=10, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)


def get_kafka_producer():
    logging.info(f"Connecting to kafka")
    config = {'bootstrap.servers': kafka_brokers}
    return confluent_kafka.Producer(**config)


if __name__ == "__main__":  
    producer = get_kafka_producer()
    create_topic()
    for message in sys.stdin:
        if message != '\n':
            failed = True
            while failed:
                try:
                    producer.produce(topic_name, value=bytes(message, encoding='utf8'))
                    failed = False
                except BufferError as e:
                    producer.flush()
                
        else:
            break
    producer.flush()