import json
import boto3
import urllib3
from datetime import datetime

def lambda_handler(event, context):
    
    extracted_data = call_yelps()
    insert_data(extracted_data, table='yelp-restaurants')
    insert_data_to_ES(extracted_data, index='restaurant')

    return {
        'statusCode': 200,
        'body': json.dumps('Finished!')
    }
    


def insert_data_to_ES(data_list, index):
    
    http = urllib3.PoolManager()
    headers = urllib3.make_headers(basic_auth='master:Passw0rd#')
    headers['Content-Type'] = 'application/json'
    endpoint = 'https://search-restaurants-sdbsgufge7frswmfnex6i2nkty.us-east-1.es.amazonaws.com/_bulk'
    
    
    for data in data_list:
        
        id = data['RestaurantID']
        cusine = data['Cuisine']
        
        json_txt = '{"index": {"_index": "%s", "_id":"%s"}}\n{"RestaurantId": "%s", "Cuisine": "%s"}\n' % (index, id, id, cusine)
        
        r = http.request('POST', endpoint, headers = headers, body=json_txt)
        # print("====REQUEST START====")
        # print(r.data)
        # print("====REQUEST END====")



def call_yelps():
    
    http = urllib3.PoolManager()
    APIkey = "EoHALx9El1c_sY38BKHOPxlUW6pAugbxWxHki7mEOGmP-XBHaqWiQs7G-FJD5LRepncfjFCk_kSGxfeQrdBTwL7fayF6vi3WNtbtXSupAwvtAX_CbBF0hUElypUSYnYx"
    headers={"Authorization": f"Bearer {APIkey}"}

    data_dict = dict()
    
    # ['japanese', 'cafes', 'chinese', 'hotdogs', 'mexican']
    
    for cuisine in ['japanese', 'cafes', 'chinese', 'hotdogs', 'mexican']:
        for offset in range(0, 1000, 50):
            r = http.request('GET', f'https://api.yelp.com/v3/businesses/search?location=Manhanttan&categories={cuisine}&limit=50&offset={offset}', headers=headers)
            data = json.loads(r.data)
            

            for business in data['businesses']:
                
                business_data_extracted = {
                    "RestaurantID" : business['id'],
                    "InsertedAtTimestamp" : datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
                    "Name" : business['name'],
                    "Address" : business['location']['display_address'],
                    "Coordinates" : {
                            "latitude": str(business['coordinates']['latitude']),
                            "longitude": str(business['coordinates']['longitude'])
                        },
                    "NumberOfReviews" : business['review_count'],
                    "Rating" : str(business['rating']),
                    "ZipCode" : business['location']['zip_code'],
                    "Cuisine" : cuisine.lower()
                }
                
                data_dict[business_data_extracted['RestaurantID']] = business_data_extracted
    
    return list(data_dict.values())
    



def insert_data(data_list, table, db=None):
    
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    
    # overwrite if the same index is provided
    for data in data_list:
        response = table.put_item(Item=data)
    print('@insert_data: response', response)
    
    
    return response