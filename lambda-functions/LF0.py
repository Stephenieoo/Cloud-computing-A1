import json

def lambda_handler(event, context):

    messages = [
        {
            'type': 'unstructured',
            'unstructured': {
                'text': 'I’m still under development. Please come back later.'
            }
        }
    ]

    return {
        'statusCode': 200,
        "messages": messages

    }