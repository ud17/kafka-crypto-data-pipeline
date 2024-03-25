# consumer.py
from kafka import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
import json
import os

# loading env variables
load_dotenv()
# KAFKA
KAFKA_HOST = os.getenv('KAFKA_HOST')
KAKFA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')

# COIN LIVE
API_KEY = os.getenv('API_KEY')

# PG
PG_USER = os.getenv('PG_USER')
PG_PWD = os.getenv('PG_PWD')
PG_HOST = os.getenv('PG_HOST')
PG_DB = os.getenv('PG_DB')
PG_PORT = os.getenv('PG_PORT')

def connect_to_database():
    # Replace with your SQL database connection details
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PWD,
        database=PG_DB
    )
    conn.autocommit = True

    print('Connected to db.')
    return conn

def consume_and_store(consumer, cursor):

    for message in consumer:
        crypto = json.loads(message.value.decode('utf-8'))

        data = {
            'name': crypto['name'],
            'code': crypto['code'],
            'rank': crypto['rank'],
            'age': crypto['age'],
            'exchanges': crypto['exchanges'],
            'markets': crypto['markets'],
            'allTimeHighUSD': crypto['allTimeHighUSD'],
            'rate': crypto['rate'],
            'volume': crypto['volume'],
            'cap': crypto['cap'],
            'circulatingSupply': crypto['circulatingSupply'],
            'totalSupply': crypto['totalSupply'],
            'maxSupply': crypto['maxSupply']
        }

        query = """
            INSERT INTO coin (name, code, rank, age, png64, exchanges, markets, allTimeHighUSD, rate, volume, cap, circulatingSupply, totalSupply, maxSupply, website)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            # insert record to db
            cursor.execute(query, list(data.values()))
            print('Inserted: ' + str(crypto['name']) + ' | ', datetime.now())
        except Exception as e:
            print('Something went wrong: ', e)

    

def main():
    kafka_bootstrap_servers = KAFKA_HOST
    kafka_topic = KAKFA_TOPIC
    consumer_group_id = KAFKA_GROUP_ID
    consumer = KafkaConsumer(kafka_topic, group_id=consumer_group_id, bootstrap_servers=kafka_bootstrap_servers)
    
    try:
        engine = connect_to_database()
        consume_and_store(consumer, engine.cursor())
    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()
