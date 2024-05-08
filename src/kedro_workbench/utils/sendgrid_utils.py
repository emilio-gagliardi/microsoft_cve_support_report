from sendgrid import SendGridAPIClient
# from kedro.config import ConfigLoader
# from kedro.framework.project import settings
# from kedro.framework.session import KedroSession
# from kedro.framework.startup import bootstrap_project
# from pathlib import Path
# import os

# from kedro.config import ConfigLoader
# from kedro.framework.project import settings
import json
import base64
# conf_loader = ConfigLoader(settings.CONF_SOURCE)
# credentials = conf_loader["credentials"]
# sendgrid_credentials = credentials["sendgrid"]
# sendgrid_username = sendgrid_credentials["username"]
# sendgrid_password = sendgrid_credentials["password"]
# sendgrid_api_key = sendgrid_credentials["api_key"]

def get_all_lists(api_key):
    sg = SendGridAPIClient(api_key)
    response = sg.client.marketing.lists.get()
    if response.status_code == 200:
        lists_data = json.loads(response.body)
        return lists_data.get('result')
    else:
        raise Exception(f"Failed to fetch SendGrid lists: {response.status_code}")
    
def get_recipients_from_sendgrid_list(api_key, list_id):
    sg = SendGridAPIClient(api_key)
    # Ensuring that the list_id is passed as a list or appropriately formatted string
    query_params = {'list_ids': list_id}
    response = sg.client.marketing.contacts.get(query_params=query_params)
        
    if response.status_code == 200:
        data = json.loads(response.body)
        # print(f"API response data:\n{json.dumps(data, indent=4)}")
        # Filter recipients based on list_id
        recipients = [
            contact['email'] 
            for contact in data.get('result', []) 
            if list_id in contact['list_ids'] and contact['list_ids']
        ]
        return recipients
    else:
        raise Exception(f"Failed to fetch contacts from SendGrid list: {response.status_code}")
    
def load_encoded_file(file_path):
    """Load and encode the file content."""
    with open(file_path, 'rb') as file:
        return base64.b64encode(file.read()).decode()