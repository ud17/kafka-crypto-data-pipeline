import json
import urllib.parse
import boto3
import uuid
from datetime import datetime
from decimal import Decimal

print('Loading function')

s3 = boto3.client('s3')
dynamodb = boto3.resource("dynamodb")
coins_table = dynamodb.Table('coins')
prices_table = dynamodb.Table('prices')

def insert_into_prices(table, crypto):
    print('`prices` table status:', table.table_status)
    
    try:
        coin = {
            'id': crypto['id'],
            'name': crypto['name'],
            'code': crypto['code'],
            'exchanges': crypto['exchanges'],
            'markets': crypto['markets'],
            'rate': crypto['rate'],
            'volume': crypto['volume'],
            'cap': crypto['cap'],
            'circulatingSupply': crypto['circulatingSupply'],
            'totalSupply': crypto['totalSupply'],
            'maxSupply': crypto['maxSupply'],
            'timestamp': crypto['timestamp']
        }
        
        table.put_item(Item = coin)
        print(f"insert_into_prices: info: {coin['name']} inserted.")
    except Exception as e:
        print('insert_into_prices: error:', e)
        raise e
    
    
def insert_into_coins(table, crypto):
    print('`coins` table status:', table.table_status)
    
    try:
        coin = {
            'name': crypto['name'],
            'code': crypto['code'],
            'rank': crypto['rank'],
            'age': crypto['age'],
            'pairs': crypto['pairs'],
            'exchanges': crypto['exchanges'],
            'png32': crypto['png32'],
            'png64': crypto['png64'],
            'allTimeHighUSD': crypto['allTimeHighUSD'],
            'circulatingSupply': crypto['circulatingSupply'],
            'totalSupply': crypto['totalSupply'],
            'maxSupply': crypto['maxSupply'],
            'updatedAt': crypto['timestamp']
        }
        
        coin['symbol'] = crypto['symbol'] if 'symbol' in crypto and crypto['symbol'] is not None else "N/A"
        
        # replace with newest data
        table.put_item(Item = coin)
        print(f"insert_into_coins: info: {coin['name']} updated.")
    except Exception as e:
        print('insert_into_coins: error:', e)
        raise e
    
def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        object = s3.get_object(Bucket=bucket, Key=key)
        print('Response', object)
        raw_data = object['Body'].read().decode('utf-8')
        coin = json.loads(raw_data, parse_float=Decimal)
        
        insert_into_prices(prices_table, coin)
        insert_into_coins(coins_table, coin)
        
    except Exception as e:
        print(f"lambda_handler: error: {e}")
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e