#########################################################
# # Code modified from user_posting_emulation.py
# Requests changed from POSTs to PUTs
# API now interacts with Kinesis rather than Kafka
#########################################################

import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from datetime import datetime

# useful to encode datetime entries in JSON
def convert_datetime(obj):
    if isinstance(obj,datetime):
        return obj.isoformat() # converts if datetime
    return obj                 # else passes

random.seed(42)

# to open externally stored info
with open('db_creds.yaml','r') as f:
    creds = yaml.safe_load(f)
with open('constants.yaml','r') as t:
    consts = yaml.safe_load(t)

INVOKE_URL = consts['INVOKE_URL']

# streaming URL for single record PUT

class AWSDBConnector:

    def __init__(self):
        # following refer to credentials in my
        # db_creds.yaml file ignored by git
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            # sending pin data to the .pin stream

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                pin_result = {key: convert_datetime(val) for key,val in pin_result.items()}
                pin_payload = json.dumps({
                    # Kinesis json structure
                    "Data": pin_result,
                    # not "record": {"index": pin_result} as w/ Kafka
                    "PartitionKey": "partition-1"
                })
            headers = {'Content-Type': 'application/json'}  # not /vnd.kafka.json.v2+json   
            pin_response = requests.request("PUT",
                                        INVOKE_URL.format('pin'),
                                        headers=headers,
                                        data=pin_payload
                                        )
            
            # sending geo data to the .geo stream

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_result = {key: convert_datetime(val) for key,val in geo_result.items()}
                geo_payload = json.dumps({
                    # Kinesis json structure
                    "Data": geo_result,
                    # not "record": {"index": geo_result} as w/ Kafka
                    "PartitionKey": "partition-1"
                })
            headers = {'Content-Type': 'application/json'}  # not /vnd.kafka.json.v2+json  
            geo_response = requests.request("PUT",
                                        INVOKE_URL.format('geo'),
                                        headers=headers,
                                        data=geo_payload
                                        )
            
            # sending user data to the .user stream

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_result = {key: convert_datetime(val) for key,val in user_result.items()}
                user_payload = json.dumps({
                    # Kinesis json structure
                    "Data": user_result,
                    # not "record": {"index": user_result} as w/ Kafka
                    "PartitionKey": "partition-1" 
                })
            headers = {'Content-Type': 'application/json'}  # not /vnd.kafka.json.v2+json
            user_response = requests.request("PUT",
                                        INVOKE_URL.format('user'),
                                        headers=headers,
                                        data=user_payload
                                        )

            print(
                [
                    'CODES: ',
                    pin_response.status_code,
                    geo_response.status_code,
                    user_response.status_code
                 ]
                )


if __name__ == "__main__":
    print('Post stream ongoing...')
    run_infinite_post_data_loop()