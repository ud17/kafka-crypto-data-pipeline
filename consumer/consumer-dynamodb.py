# consumer.py
from kafka import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime
from decimal import Decimal
import boto3
import uuid
import json
import os

# loading env variables
load_dotenv()
# KAFKA
KAFKA_HOST = os.getenv('KAFKA_HOST')
KAKFA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')

# DynamoDB
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

def connect_to_database():
    # Replace with your SQL database connection details
    dynamodb = boto3.resource(
            service_name = 'dynamodb', 
            region_name='ca-central-1', 
            aws_access_key_id = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY
        )
    print('Connected to Dynamodb.')
    table = dynamodb.Table('coins')
    print(f"Table status: {table.table_status}")
    return table

def consume_and_store(consumer, table):

    for message in consumer:
        crypto = json.loads(message.value.decode('utf-8'), parse_float=Decimal)

        uuid_coin = str(uuid.uuid4())
        data = {
            'id': uuid_coin,
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
            'maxSupply': crypto['maxSupply'],
        }

        try:
            # insert record to db
            table.put_item(Item = data)
            print('Inserted: ' + str(crypto['name']) + ' | ', datetime.now())
        except Exception as e:
            print('Something went wrong: ', e)

    

def main():
    kafka_bootstrap_servers = KAFKA_HOST
    kafka_topic = KAKFA_TOPIC
    consumer_group_id = KAFKA_GROUP_ID
    consumer = KafkaConsumer(kafka_topic, group_id=consumer_group_id, bootstrap_servers=kafka_bootstrap_servers)
    
    try:
        table = connect_to_database()
        consume_and_store(consumer, table)
    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()
