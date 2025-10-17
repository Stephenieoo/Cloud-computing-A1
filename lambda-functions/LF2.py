import json
import boto3
import random
import base64
import urllib.request

# Configuration

# AWS Region and resource endpoints
REGION = 'us-east-1'
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/676206931048/DiningQueueQ1'
FROM_EMAIL = 'xo258@nyu.edu'

# OpenSearch credentials (fine-grained access control)
OPENSEARCH_HOST = 'search-restaurants-domain-l7g3jfsjjguyni3v5j3vblurkq.us-east-1.es.amazonaws.com'
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASSWORD = 'Admin@12345'

# AWS Client Initialization

# Create clients for AWS services
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
table = dynamodb.Table('yelp-restaurants')

def search_restaurants(cuisine):
    """ Search for restaurants in OpenSearch by cuisine type.
    Args:
        cuisine (str): The type of cuisine to search for (e.g., 'Italian', 'Chinese').
    Returns:
        list: A list of up to 5 randomly selected restaurant IDs. """
    url = f'https://{OPENSEARCH_HOST}/restaurants/_search'
    query = {"query": {"match": {"Cuisine": cuisine}}, "size": 100}
    
    # Encode credentials for Basic Authentication
    credentials = base64.b64encode(f'{OPENSEARCH_USER}:{OPENSEARCH_PASSWORD}'.encode()).decode()
    headers = {'Authorization': f'Basic {credentials}', 'Content-Type': 'application/json'}
    
    try:
        req = urllib.request.Request(url, json.dumps(query).encode(), headers, method='POST')
        result = json.loads(urllib.request.urlopen(req).read())
        # Extract restaurant IDs from search hits
        ids = [hit['_source']['RestaurantID'] for hit in result['hits']['hits']]
        return random.sample(ids, min(5, len(ids))) if ids else []
    except Exception as e:
        print(f'OpenSearch Error: {e}')
        return []

def get_restaurant_details(restaurant_ids):
    """ Retrieve full restaurant details from DynamoDB by BusinessID.
    Args:
        restaurant_ids (list): A list of restaurant IDs retrieved from OpenSearch.
    Returns:
        list: A list of restaurant detail dictionaries. """
    restaurants = []
    for rid in restaurant_ids:
        try:
            response = table.get_item(Key={'BusinessID': rid})
            if 'Item' in response:
                item = response['Item']
                restaurants.append({
                    'name': item.get('Name', 'N/A'),
                    'address': item.get('Address', 'N/A'),
                    'rating': item.get('Rating', 'N/A'),
                    'num_reviews': item.get('NumReviews', 'N/A'),
                    'zip_code': item.get('ZipCode', 'N/A'),
                    'coordinates': item.get('Coordinates', {}),
                    'cuisine': item.get('Cuisine', 'N/A')
                })
        except Exception as e:
            print(f'DynamoDB Error for {rid}: {e}')
    print(f'Retrieved {len(restaurants)} restaurants')
    return restaurants

def send_email(to_email, restaurants, cuisine, location, time, people):
    """ Send restaurant recommendation email via AWS SES.
    Returns: bool: True if email sent successfully, False otherwise. """
    subject = f"Your {cuisine} Restaurant Recommendations"
    
    body = f"Hello!\n\nHere are my {cuisine} restaurant suggestions for {people} people, for {time} in {location}:\n\n"
    
    # Format each restaurant entry in email body
    for i, r in enumerate(restaurants, 1):
        body += f"{i}. {r['name']}\n"
        body += f"   Address: {r['address']}\n"
        body += f"   Rating: {r['rating']}/5 ({r['num_reviews']} reviews)\n"
        body += f"   Zip Code: {r['zip_code']}\n"
        coords = r.get('coordinates', {})
        if coords and isinstance(coords, dict):
            lat = coords.get('latitude', 'N/A')
            lon = coords.get('longitude', 'N/A')
            body += f"   Coordinates: {lat}, {lon}\n"
        
        body += "\n"
    
    body += "Enjoy your meal!"
    
    try:
        response = ses.send_email(
            Source=FROM_EMAIL,
            Destination={'ToAddresses': [to_email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        print(f'✅ Email sent. MessageId: {response["MessageId"]}')
        return True
    except Exception as e:
        print(f'❌ SES Error: {e}')
        return False

def lambda_handler(event, context):
    """ AWS Lambda handler for processing dining requests.

    - Reads messages from SQS queue.
    - Extracts dining preferences (Cuisine, Email, etc.).
    - Searches OpenSearch for matching restaurants.
    - Retrieves detailed info from DynamoDB.
    - Sends an email with recommendations via SES.

    Args:
        event (dict): Lambda event data (unused here since SQS is the trigger source).
        context (object): Lambda context object.

    Returns:
        dict: Summary of processed and failed message counts. """
    print('Lambda triggered')
    
    # Step 1: Receive messages from SQS
    try:
        response = sqs.receive_message(QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=10)
    except Exception as e:
        print(f'SQS Error: {e}')
        return {'statusCode': 500, 'body': 'SQS Error'}
    
    messages = response.get('Messages', [])
    
    if not messages:
        print('No messages in queue')
        return {'statusCode': 200, 'body': 'No messages'}
    
    print(f'Received {len(messages)} messages')
    
    processed = 0
    failed = 0
    
    # Step 2: Process each message
    for message in messages:
        try:
            data = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']
            print(f'Processing: {data}')
            
            cuisine = data.get('Cuisine')
            email = data.get('Email')
            location = data.get('Location')
            dining_time = data.get('DiningTime')
            num_people = data.get('NumberOfPeople')
            
            # # Validate essential fields
            # if not cuisine or not email:
            #     print(f'Missing fields: Cuisine={cuisine}, Email={email}')
            #     sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            #     failed += 1
            #     continue
            
            # Step 3: Search and retrieve data
            restaurant_ids = search_restaurants(cuisine)
            if not restaurant_ids:
                print(f'No restaurants found for: {cuisine}')
                sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
                failed += 1
                continue
            
            restaurants = get_restaurant_details(restaurant_ids)
            if not restaurants:
                print('No details retrieved')
                sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
                failed += 1
                continue
            
            # Step 4: Send email
            if send_email(email, restaurants, cuisine, location, dining_time, num_people):
                sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
                processed += 1
                print(f'✅ Processed for {email}')
            else:
                print(f'❌ Failed for {email}')
                failed += 1
                
        except Exception as e:
            print(f'Error: {e}')
            import traceback
            print(traceback.format_exc())
            failed += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps({'processed': processed, 'failed': failed})
    }