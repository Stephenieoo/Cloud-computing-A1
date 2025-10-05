import json
import boto3

sqs = boto3.client('sqs')
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/676206931048/DiningQueueQ1"

def lambda_handler(event, context):
    slots = event["sessionState"]["intent"]["slots"]
    # check whether collect all the slots
    if not all(slots.values()):
        return {
            "sessionState": {
                "dialogAction": {"type": "Delegate"},
                "intent": event["sessionState"]["intent"]
            }
        }
    # get value from slots
    
    location = slots["Location"]["value"]["interpretedValue"]
    cuisine = slots["Cuisine"]["value"]["interpretedValue"]
    diningtime = slots["DiningTime"]["value"]["interpretedValue"]
    num_people = slots["NumberOfPeople"]["value"]["interpretedValue"]
    email = slots["Email"]["value"]["interpretedValue"]

    message = {
        "Location": location,
        "Cuisine": cuisine,
        "DiningTime": diningtime,
        "NumberOfPeople": num_people,
        "Email": email
    }
    
    # send to SQS
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message)
    )
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": event["sessionState"]["intent"]
        }
    }


