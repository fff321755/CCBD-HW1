import json
import boto3


def lambda_handler(event, context):

    client = boto3.client('lex-runtime')
    event_body = json.loads(event['body'])
    msg_from_user = event_body['messages'][0]['unstructured']['text']
    
    response = client.post_text(botName='DiningConcierge',
                                botAlias='dine',
                                userId='testuser',
                                inputText=msg_from_user)
                                
    
    response_text = response['message']

    body = {}

    body['messages'] = [{ 
                'type' : 'unstructured',
                'unstructured' : {'text': response_text}
                }]
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'
        },

        'body': json.dumps(body)
    }
    
    
    
    