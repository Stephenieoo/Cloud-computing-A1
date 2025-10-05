import json
import boto3

def lambda_handler(event, context):
    print("**********event:", event)
    print("**********context:", context)
    #intent_name = event['sessionState']['intent']['name']
    #print("**********intent_name:", intent_name)
    user_input = event['messages'][0]['unstructured']['text']
    client = boto3.client('lexv2-runtime')

    response = client.recognize_text(
                botId='6XN65O4A4L',  # Replace with your Lex bot ID
                botAliasId='TSTALIASID', # Replace with your Lex bot alias ID (e.g., 'TestBotAlias')
                localeId='en_US', # Or your specific locale
                sessionId='111', # A unique identifier for the conversation
                text=user_input
                )
    
    messages = [
        {
            'type': 'unstructured',
            'unstructured': {
                'text': response['messages'][0]['content']
            }
        }
    ]

    return {
        'statusCode': 200,
        "messages": messages
    }
