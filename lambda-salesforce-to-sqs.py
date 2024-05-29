import json
import boto3
import os 
import sys
import urllib3 
from urllib.parse import urlencode

# Salesforce Parameters
salesforce_params = {
    "grant_type": "password",
    "client_id": os.environ['SALESFORCE_CLIENT_ID'],
    "client_secret": os.environ['SALESFORCE_CLIENT_SECRET'],
    "username": os.environ['SALESFORCE_USERNAME'],
    "password": os.environ['SALESFORCE_PASSWORD']
}

# AWS Parameters/Variables
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID_1']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY_1']
sqs = boto3.client('sqs', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
sqs_queue_url = os.environ['AWS_SQS_QUEUE_URL']
MAX_BATCH_SIZE = 200000  # Maximum size of the batch in bytes

http = urllib3.PoolManager()

# Salesforce API token
def get_sf_token():
    try:
        # Encode the parameters
        encoded_params = urlencode(salesforce_params)
        
        # URL for the POST request
        url = "https://login.salesforce.com/services/oauth2/token"
        
        # Make the POST request using urllib3
        response = http.request(
            'POST',
            url,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            body=encoded_params,
            timeout=30.0
        )
        
        # Check the response status
        if response.status >= 300:
            raise Exception(f'API error when calling {url}: {response.data.decode("utf-8")}')
        
        # Parse the JSON response
        response_data = json.loads(response.data.decode('utf-8'))
        
        # Extract the access token and instance URL
        access_token = response_data.get("access_token")
        instance_url = response_data.get("instance_url")
        
        return access_token, instance_url
    except Exception as e:
        print(f"Error getting Token from Salesforce: {str(e)}")
        return None, None

# Salesforce API call
def sf_api_call(action, parameters={}, method='GET', access_token=None, instance_url=None):
    if instance_url is not None and access_token is not None: 
        headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'Authorization': f'Bearer {access_token}'
        }
    
        # Build the URL with parameters if method is GET
        if method.upper() == 'GET':
            url = f'{instance_url}{action}?{urlencode(parameters)}'
            body = None
        else:
            url = f'{instance_url}{action}'
            body = urlencode(parameters)

        # Make the request using urllib3
        try:
            response = http.request(
                method.upper(),
                url,
                headers=headers,
                body=body,
                timeout=30.0
            )
        except urllib3.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            exit(1)  # Error, no API call
        
        # Check the response status
        if response.status >= 300:
            raise Exception(f'API error when calling {url}: {response.data.decode("utf-8")}')
        else:
            return json.loads(response.data.decode('utf-8'))
    else:
        print("access_token and instance_url must be valid Strings")
        exit(1)  # Error, invalid token or instance URL

# Method used to send the result of the Salesforce API to an SQS queue
def send_json_to_sqs(sqs, json_message, sqs_queue_url):
    try:
        # Convert JSON object to a JSON-formatted string
        json_message_str = json.dumps(json_message)
        # Send the JSON object as a single message to SQS
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=json_message_str, MessageGroupId='sfdc-elk-serverless')

    except Exception as e:
        print(f"Error sending JSON to SQS: {str(e)}")
        
# Lambda Handler
def lambda_handler(event, context):
    try:
        access_token, instance_url = get_sf_token()

        response_data = sf_api_call('/services/data/v55.0/query/', {
            'q': 'select Action,CreatedById,CreatedDate,Display,Id,ResponsibleNamespacePrefix,Section,CreatedBy.name FROM SetupAuditTrail WHERE CreatedDate = N_DAYS_AGO:1'
        }, access_token=access_token, instance_url=instance_url)
 
        json_records = response_data.get("records", [])  # Assuming the response_data is a dictionary
    
        # Accumulate events in a list
        events_batch = []
        current_batch_size = 0
        final = False
    
        if response_data.get("done"):
            final = True
    
        while response_data.get("nextRecordsUrl") or final == True:
            if response_data.get("done") or final == True: # making sure the final/last page is considered
                final = False
            for index, event in enumerate(json_records, start=1):
                try:
                    # Convert the event to a JSON-formatted string
                    event_str = json.dumps(event, separators=(',', ':'))
                
                    # Add the size of the event string to the current batch size
                    current_batch_size += sys.getsizeof(event_str)

                    # Add each event to the lines batch
                    events_batch.append(event)

                    # Check if the current batch size is large enough to create a JSON object
                    if current_batch_size >= MAX_BATCH_SIZE or index == len(json_records):
                        # Create a JSON object containing lines from the log file
                        json_message = {'message': events_batch}
                        send_json_to_sqs(sqs, json_message, sqs_queue_url)

                        # Reset the lines batch and size
                        events_batch = []
                        current_batch_size = 0

                except Exception as sqs_error:
                    print(f"Error processing events: {str(sqs_error)}")
        
            if response_data.get("nextRecordsUrl"): # run the pagination only if there are still pages to get
                response_data = sf_api_call(response_data.get("nextRecordsUrl"), access_token=access_token, instance_url=instance_url)
                json_records = response_data.get("records", [])
                if response_data.get("done"): # making sure the final/last page is considered
                    final = True

        print("SUCCESS")
        return {"statusCode": 200, "body": "SUCCESS"}
    
    except Exception as e:
        print("Error:", e)
        return {"statusCode": 500, "body": "FAILED"}
