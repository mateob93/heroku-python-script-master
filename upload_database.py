import json
import os

import yaml
from model import package
import requests
import db_credentials
import mqtt_client
import base64

from config import BASE_ENDPOINT


def parse_model(topic, msg):
    pack_as_dict = yaml.safe_load(msg)
    pack = package.Package(topic, **pack_as_dict)
    return pack


def form_data(package, credentials):
    s = {"Db_connection": credentials.to_dict(),
         "package": package.to_dict()}
    return json.dumps(s)


def upload_to_database(topic, msg):
    try:
        package_to_send = parse_model(topic, msg)
    except Exception as e:
        print("Data was incomplete and model was not created:")
        print(e)
        return -1

    if topic == mqtt_client.Topics.SEISMIC.value:
        db_creds = db_credentials.DbCredentials(base64.b64decode(os.environ['SEISMIC_USR']).decode('utf-8'),
                                                base64.b64decode(os.environ['SEISMIC_SCT']).decode('utf-8'))
        endpoint = BASE_ENDPOINT + "seismic-data"
    elif topic == mqtt_client.Topics.T_AND_H.value:
        db_creds = db_credentials.DbCredentials(base64.b64decode(os.environ['TH_USR']).decode('utf-8'),
                                                base64.b64decode(os.environ['TH_SCT']).decode('utf-8'))
        endpoint = BASE_ENDPOINT + "temp-hum"
    elif topic == mqtt_client.Topics.ULTRASOUND.value:
        db_creds = db_credentials.DbCredentials(base64.b64decode(os.environ['ULTRASOUND_USR']).decode('utf-8'),
                                                base64.b64decode(os.environ['ULTRASOUND_SCT']).decode('utf-8'))
        endpoint = BASE_ENDPOINT + "ultrasound"
    elif topic == mqtt_client.Topics.EXAMPLE.value:
        db_creds = db_credentials.DbCredentials(base64.b64decode(os.environ['EX_USR']).decode('utf-8'),
                                                base64.b64decode(os.environ['EX_SCRT']).decode('utf-8'))
        endpoint = BASE_ENDPOINT + "example"

    data = form_data(package_to_send, db_creds)
    r = requests.post(url=endpoint, data=data, headers={'content-type': 'application/json'})
    print(r.text)
    return r.status_code


if __name__ == "__main__":
    with open('example_files/full_package_yml.yml', 'r') as f:
        ms = f.read()
    top = "/pi/temp"

    upload_to_database(top, ms)
