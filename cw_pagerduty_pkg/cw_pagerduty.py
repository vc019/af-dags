import requests
import json

# Update to match your API key
API_KEY = '6ko9B8K5yCbMmETsNoaV'
SERVICE_ID = 'PG7SVKW'
FROM = 'vipin.chadha@gmail.com'


def get_priorities(prority_cd):
    url = 'https://api.pagerduty.com/priorities?limit=10&offset=10&total=false'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/vnd.pagerduty+json;version=2',
        'Authorization': 'Token token={token}'.format(token=API_KEY),
        'From': FROM
    }

    response = requests.get(url=url, headers=headers)
    priorities = response.json().get("priorities")
    for priority in priorities:
        print(priority["id"], priority["summary"])
        if priority["summary"] == prority_cd:
            return priority["id"]

    return "not_found"


def trigger_incident(incident_title, incident_details, incident_priority):
    url = 'https://api.pagerduty.com/incidents'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/vnd.pagerduty+json;version=2',
        'Authorization': 'Token token={token}'.format(token=API_KEY),
        'From': FROM
    }
    priority_id = get_priorities(incident_priority)
    payload = {
        "incident": {
            "type": "cloudwalker incident",
            "title": incident_title,
            "service": {
                "id": SERVICE_ID,
                "type": "service_reference"
            },
            "body": {
                "type": "incident_body",
                "details": incident_details
            },
            "urgency": "high",
            "priority": {"id": priority_id,
                         "type": "priority"
                         }
        }
    }

    r = requests.post(url, headers=headers, data=json.dumps(payload))

    print('Incident created, Status Code: {code}'.format(code=r.status_code))
    #print(r.json())


if __name__ == '__main__':
    trigger_incident("This is the incident title", "caused some incident", "P1")
