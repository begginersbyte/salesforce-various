import json
import sys
import urllib3
from urllib.parse import urlencode

http = urllib3.PoolManager()
MAX_BATCH_SIZE = 200000  # Maximum size of the batch in bytes...
                         # this is not totally needed, I created it to integrate it with SQS,
                         # but I didn't want to re-create the logic of the script so I'll just leave it here.

salesforce_params = {
    "grant_type": "password",
    "client_id": "",
    "client_secret": "",
    "username": "",
    "password": ""
}

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
        
        # Print the access token and instance URL (for debugging purposes)
        print(access_token)
        print(instance_url)
        
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

try:
    access_token, instance_url = get_sf_token()

    response_data = sf_api_call('/services/data/v55.0/query/', {
        'q': 'select Action,CreatedById,CreatedDate,Display,Id,ResponsibleNamespacePrefix,Section,CreatedBy.name FROM SetupAuditTrail WHERE CreatedDate > N_DAYS_AGO:7'
    }, access_token=access_token, instance_url=instance_url)

    json_records = response_data.get("records", [])  # Assuming the response_data is a dictionary
    
    # Accumulate events in a list
    events_batch = []
    current_batch_size = 0
    final = False

    if response_data.get("done"): # all the logic behind the "final" variable can be improved.
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
                    
                    with open("C:\\Users\\kevin\\Desktop\\output.txt", "a", encoding="utf-8") as f:
                        f.write(str((json_message)))

                    # Reset the lines batch and size
                    events_batch = []
                    current_batch_size = 0

            except Exception as e:
                print(f"Error processing events: {str(e)}")
        
        if response_data.get("nextRecordsUrl"): # run the pagination only if there are still pages to get
            response_data = sf_api_call(response_data.get("nextRecordsUrl"), access_token=access_token, instance_url=instance_url)
            json_records = response_data.get("records", [])
            if response_data.get("done"): # making sure the final/last page is considered
                final = True

    print("SUCCESS")

except Exception as e:
    print("Error:", e)
