# producer.py
import time
from kafka import KafkaProducer
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
KAFKA_HOST = os.getenv('KAFKA_HOST')
KAKFA_TOPIC = os.getenv('KAFKA_TOPIC')

def fetch_crypto_prices():
    # Replace 'your_coin_ids' with the actual coin IDs you want to fetch
    coin_ids = ['bitcoin', 'ethereum']
    url = f'https://api.coingecko.com/api/v3/simple/price?ids={"%2C".join(coin_ids)}&vs_currencies=usd'
    response = requests.get(url)
    return response.json()

def produce_to_kafka(producer, topic):
    while True:
        print('START')
        crypto_prices = fetch_crypto_prices()
        # Serialize data to bytes (assuming it's JSON)
        print('crypto_prices: ' + str(crypto_prices))
        message_value = json.dumps(crypto_prices).encode('utf-8')
        producer.send(topic, value=message_value)
        print('END')
        time.sleep(5)  # Fetch data every 5 seconds

def main():
    kafka_bootstrap_servers = KAFKA_HOST
    kafka_topic = 'crypto_prices'
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    try:
        produce_to_kafka(producer, kafka_topic)
    except KeyboardInterrupt:
        producer.close()

if __name__ == "__main__":
    main()