import json
import boto3
import random
import os
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import urllib.request
import urllib.parse

# 配置
REGION = 'us-east-1'
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/676206931048/DiningQueueQ1'
FROM_EMAIL = 'xo258@nyu.edu'
OPENSEARCH_HOST = 'search-restaurants-domain-l7g3jfsjjguyni3v5j3vblurkq.us-east-1.es.amazonaws.com'
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASSWORD = 'Admin@12345'

# AWS客户端
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
table = dynamodb.Table('yelp-restaurants')

def search_restaurants(cuisine):
    """从OpenSearch搜索餐厅"""
    url = f'https://{OPENSEARCH_HOST}/restaurants/_search'
    query = {
        "query": {"match": {"Cuisine": cuisine}},
        "size": 100
    }
    
    # 使用基本认证（Fine-grained access control）
    import base64
    credentials = f'{OPENSEARCH_USER}:{OPENSEARCH_PASSWORD}'
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/json'
    }
    
    data = json.dumps(query).encode('utf-8')
    
    try:
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            hits = result['hits']['hits']
            ids = [hit['_source']['RestaurantID'] for hit in hits]
            return random.sample(ids, min(5, len(ids)))
    except Exception as e:
        print(f'Error: {e}')
        return []

def get_restaurant_details(restaurant_ids):
    """从DynamoDB获取详情"""
    restaurants = []
    for rid in restaurant_ids:
        try:
            item = table.get_item(Key={'BusinessID': rid})['Item']
            restaurants.append({
                'name': item.get('Name', 'N/A'),
                'address': item.get('Address', 'N/A'),
                'rating': float(item.get('Rating', 0)),
                'reviews': int(item.get('NumberOfReviews', 0))
            })
        except:
            pass
    return restaurants

def send_email(to_email, restaurants, cuisine, location, time, people):
    """发送推荐邮件"""
    subject = f"Your {cuisine} Restaurant Recommendations"
    
    body = f"Hello!\n\nHere are {cuisine} restaurants for {people} people at {time} in {location}:\n\n"
    for i, r in enumerate(restaurants, 1):
        body += f"{i}. {r['name']}\n   {r['address']}\n   Rating: {r['rating']}/5 ({r['reviews']} reviews)\n\n"
    body += "Enjoy your meal!"
    
    ses.send_email(
        Source=FROM_EMAIL,
        Destination={'ToAddresses': [to_email]},
        Message={
            'Subject': {'Data': subject},
            'Body': {'Text': {'Data': body}}
        }
    )

def lambda_handler(event, context):
    """主函数"""
    # SQS触发时，消息在event中
    # EventBridge触发时，需要手动从SQS读取
    
    if 'Records' in event:
        # SQS触发 - 消息在event中 ✅
        records = event['Records']
    else:
        # EventBridge触发 - 需要从SQS读取
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1
        )
        records = []
        for msg in response.get('Messages', []):
            records.append({
                'body': msg['Body'],
                'receiptHandle': msg['ReceiptHandle']
            })
    
    if not records:
        return {'statusCode': 200, 'body': 'No messages'}
    
    # 处理消息
    for record in records:
        body = json.loads(record.get('body', record.get('Body')))
        cuisine = body.get('cuisine')
        email = body.get('email')
        
        if not cuisine or not email:
            # SQS触发时自动删除，不需要手动删除
            if 'Records' not in event:
                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=record['receiptHandle']
                )
            continue
        
        # 搜索餐厅
        restaurant_ids = search_restaurants(cuisine)
        if not restaurant_ids:
            if 'Records' not in event:
                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=record['receiptHandle']
                )
            continue
        
        # 获取详情
        restaurants = get_restaurant_details(restaurant_ids)
        
        # 发送邮件
        send_email(
            email,
            restaurants,
            cuisine,
            body.get('location', 'Manhattan'),
            body.get('dining_time', body.get('time', '7:00 PM')),
            body.get('num_people', body.get('people', '2'))
        )
        
        # EventBridge触发时需要手动删除消息
        # SQS触发时自动删除
        if 'Records' not in event:
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=record['receiptHandle']
            )
    
    return {'statusCode': 200, 'body': 'Success'}