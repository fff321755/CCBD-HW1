import json
import boto3


def lambda_handler(event, context):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='RestaurantSuggestionQueue')
    
    response = queue.send_message(MessageBody= json.dumps(event['currentIntent']['slots']))
    
    
    slots= event['currentIntent']["slots"]
    # phoneNumber = slots['PhoneNumber']
    email = slots['Email']
    retStr= f"You’re all set. Expect my suggestions shortly! Have a good day. We will sent an message to {email}"
    
    return {
        "dialogAction": {
        "type": "Close",
        "fulfillmentState": "Fulfilled",
        "message": {
            "contentType": "PlainText",
            "content": retStr
        },
    }}
    
