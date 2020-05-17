import json
import yaml
from model import package
import requests
import db_credentials
import mqtt_client

DB_ENDPOINT = "http://127.0.0.1:5000/temp-hum"


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
        print("Data was incomplete and model was not created.")
        return -1

    # todo: Descubrir como esconder esto
    if topic == mqtt_client.Topics.SEISMIC.value:
        db_creds = db_credentials.DbCredentials("rqalxjjwytlbpa",
                                            "ae2da8dade7014ca5e1f6cc5af99b995ebc39f47035079b316e2101b9cdcba78")
    elif topic == mqtt_client.Topics.T_AND_H.value:
        db_creds = db_credentials.DbCredentials("fholeiikypyjvb", "37ef2dbb2386e0fd3393b611ad251d8d9e565fadf460509e5b965e0f4242a5c2")

    data = form_data(package_to_send, db_creds)
    r = requests.post(url=DB_ENDPOINT, data=data, headers={'content-type': 'application/json'})
    print(r.text)
    return r.status_code


if __name__ == "__main__":
    with open('example_files/full_package_yml.yml', 'r') as f:
        ms = f.read()
    top = "/pi/temp"

    upload_to_database(top, ms)
