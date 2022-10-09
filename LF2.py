import json
import boto3
import urllib3
from boto3.dynamodb.conditions import Attr


def lambda_handler(event, context):
    suggestionNum = 3
    message_list = receiveSQSMessage()
    for message in message_list: 
        RestaurantID_list = open_search(message['Cuisine'], suggestionNum, 'restaurant')
        RestaurantInfo_list = read_dynomalDB(RestaurantID_list, table='yelp-restaurants')
        sendMessageToCustomer(message, RestaurantInfo_list, message['Email'])

    return {
        'statusCode': 200,
        'body': json.dumps(f"Finish {len(message_list)} Suggestions!")
    }
    
    

def sendMessageToCustomer(message, RestaurantInfo_list, email = 'fff321755@gmail.com'):
    
    ses_client = boto3.client('ses')
    # response = ses_client.verify_email_address(EmailAddress=email)
    
    email_plaintxt = f"Hello! Here are my {message['Cuisine']} restaurant suggestions for message {message['NumberofPeople']} people, for {message['Date']} at {message['Time']}.\n"
    
    
    for RestaurantInfo in RestaurantInfo_list:
        
        email_plaintxt += f'''  
                                Restaurant Name: {RestaurantInfo['Name']}
                                Business ID: {RestaurantInfo['RestaurantID']}
                                
                                Rating: {RestaurantInfo['Rating']}
                                Number Of Reviews: {RestaurantInfo['NumberOfReviews']}
                                
                                Restaurant Address: {" ".join(RestaurantInfo['Address'])}
                                Zip Code: {RestaurantInfo['ZipCode']}
                                Coordinates:
                                    latitude: {RestaurantInfo['Coordinates']['latitude']}
                                    longitude: {RestaurantInfo['Coordinates']['longitude']}
                    
                    
                    
                                '''
               
    destination = {"ToAddresses": [email,],}
    message={"Body": {  "Text": {"Charset": "UTF-8", "Data": email_plaintxt,}},
                        "Subject": {"Charset": "UTF-8","Data": "Your Dinner Suggestions!",},}
    
    response = ses_client.send_email(Destination=destination, Message=message, Source=email)
    
    # print("====REQUEST START====")
    # print(response)
    # print("====REQUEST fEND====")
    
    
def receiveSQSMessage():
    
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='RestaurantSuggestionQueue')
    
    message_list = []
    
    for message in queue.receive_messages(MaxNumberOfMessages=10):
        body = json.loads(message.body)
        # cuisine = body['Cuisine']
        # email = body['Email']
        # cuisineEmail_list.append((cuisine, email))
        message_list.append(body)
        
        message.delete()
    
    return message_list

    
    
def open_search(cuisine_type, suggestionNum, index = 'restaurant_suggestions2'):
    
    http = urllib3.PoolManager()
    headers = urllib3.make_headers(basic_auth='master:Passw0rd#')
    headers['Content-Type'] = 'application/json'
    endpoint = f'https://search-restaurants-sdbsgufge7frswmfnex6i2nkty.us-east-1.es.amazonaws.com/{index}/_search'
    json_txt = json.dumps({
                "from" : 0, "size" : suggestionNum,
                "query": {
                    "function_score": {
                        "query": {
                            "match_phrase": {
                            "Cuisine": cuisine_type
                        }
                    },
                    "random_score": {}
            
                    }
              }
        })
    
    
    r = http.request('POST', endpoint, headers = headers, body=json_txt)
    data = json.loads(r.data)
    RestaurantID_list = [hits['_id'] for hits in data['hits']['hits']]
    
    # print("====REQUEST START====")
    # print(json.loads(r.data))
    # print("====REQUEST fEND====")
    
    return RestaurantID_list
    

    
def read_dynomalDB(RestaurantID_list, table, db=None):
    
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    
    data = table.scan(FilterExpression=Attr('RestaurantID').is_in(RestaurantID_list))
    
    return data['Items']
