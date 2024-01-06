import requests
import json
# Define các campaign
headers =  {
    'X-CleverTap-Account-Id': '4W5-565-W95Z',  # project_id
    'X-CleverTap-Passcode': 'ACO-JIZ-YXKL',  # Passcode
    'Content-Type': 'application/json; charset=utf-8',
}

params = (
    ('batch_size', '1'),
)

data = '{"event_name":"App Launched","from":20171201,"to":20171225}'

response = requests.post('https://sg1.api.clevertap.com/1/upload', headers=headers, params=params, data=data)

print(response)
print(response.json())


#response1 = requests.get('https://in1.api.clevertap.com/1/events.json', headers=headers)





# print(response)
# print(response.json())
