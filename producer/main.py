# producer.py
from kafka import KafkaProducer
from dotenv import load_dotenv
import time
import requests
import json
import os

# loading env variables
load_dotenv()
KAFKA_HOST = os.getenv('KAFKA_HOST')
KAKFA_TOPIC = os.getenv('KAFKA_TOPIC')
API_KEY = os.getenv('API_KEY')

def fetch_crypto_prices():
    
    payload = json.dumps({
        "currency": "USD",
        "sort": "rank",
        "order": "ascending",
        "offset": 0,
        "limit": 50,
        "meta": True
    })

    headers = {
        'content-type': 'application/json',
        'x-api-key': API_KEY
    }

    base_url = "https://api.livecoinwatch.com/coins/list"
    response = requests.post(base_url, headers=headers, data=payload)
    return response.json()

def produce_to_kafka(producer, topic):
    print('START')
    crypto_prices = fetch_crypto_prices()
    # Serialize data to bytes (assuming it's JSON)
    for coin in crypto_prices:
        message_value = json.dumps(coin).encode('utf-8')
        producer.send(topic, value=message_value)
    print('END')

def main():
    kafka_bootstrap_servers = KAFKA_HOST
    kafka_topic = KAKFA_TOPIC
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    try:
        produce_to_kafka(producer, kafka_topic)
    except KeyboardInterrupt:
        producer.close()

if __name__ == "__main__":
    main()