from dotenv import load_dotenv
from kafka import KafkaConsumer
from decimal import Decimal
from botocore.exceptions import ClientError
from datetime import datetime
import logging
import boto3
import json
import uuid
import os

# loading env variables
load_dotenv()
# KAFKA
KAFKA_HOST = os.getenv('KAFKA_HOST')
KAKFA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')

# S3
S3_BUCKET = os.getenv('S3_BUCKET')

ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

def connect_to_s3():
    # s3 client
    s3_client = boto3.client('s3', region_name='ca-central-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    return s3_client

def upload_file(document, s3):

    try:
        filename = f"{document['code']}_{document['id']}.json"

        # Serialize the JSON object into a JSON string
        file_str = json.dumps(document)

        s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=file_str) # s3_client.put_object(bucket, file_name, content)
        print(f'{filename} uploaded.')
    except ClientError as e:
        logging.error(e)

def consume_and_store(consumer, s3_client):

    for message in consumer:
        crypto = json.loads(message.value.decode('utf-8'))

        # generate id
        uuid_coin = str(uuid.uuid4())
        crypto['id'] = uuid_coin

        # timestamp
        timestamp = datetime.now().isoformat()
        crypto['timestamp'] = timestamp

        print(f"{crypto['name']} received.")
        upload_file(crypto, s3_client)


def main():
    kafka_bootstrap_servers = KAFKA_HOST
    kafka_topic = KAKFA_TOPIC
    consumer_group_id = KAFKA_GROUP_ID
    consumer = KafkaConsumer(kafka_topic, group_id=consumer_group_id, bootstrap_servers=kafka_bootstrap_servers)
    
    try:
        client = connect_to_s3()
        consume_and_store(consumer, client)
        print('Consumer running.')
    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()