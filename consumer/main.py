# consumer.py
from kafka import KafkaConsumer
from dotenv import load_dotenv
import time
import requests
import json
import os

# loading env variables
load_dotenv()
KAFKA_HOST = os.getenv('KAFKA_HOST')
KAKFA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
API_KEY = os.getenv('API_KEY')

def connect_to_database():
    # Replace with your SQL database connection details
    print('SQL')

def consume_and_store(consumer):
    for message in consumer:
        crypto_data = json.loads(message.value.decode('utf-8'))
        print(crypto_data['name'])

def main():
    kafka_bootstrap_servers = KAFKA_HOST
    kafka_topic = KAKFA_TOPIC
    consumer_group_id = KAFKA_GROUP_ID
    consumer = KafkaConsumer(kafka_topic, group_id=consumer_group_id, bootstrap_servers=kafka_bootstrap_servers)
    
    try:
        consume_and_store(consumer)
    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()
