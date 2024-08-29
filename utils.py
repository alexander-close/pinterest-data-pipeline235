import json
import requests
import sqlalchemy
import yaml

from datetime import datetime
from requests import Response
from sqlalchemy import text
from sqlalchemy.engine import Engine


def convert_datetime(obj):
    '''
    Custom function for quick conversion.  (Required for converting
    data to JSON strings.)
    '''
    if isinstance(obj,datetime):
        return obj.isoformat()
    return obj

def YAMLReader(yaml_file:str)->dict:
    '''
    Custom `.yaml` reader.
    '''
    with open(yaml_file+'.yaml', 'r') as f:
        output_dict = yaml.safe_load(f)
        return output_dict

def generate_post(table_key:str, row_number:int, connection)->dict:
    '''
    Generates a 'post' by selecting a row from a database table.
    Table key options are `pin`, `geo` and `user`.  The resultant row
    is converted into `dict` format and datetimes are transformed to `str`
    so they may be later converted into JSON strings.
    '''
    # try:
    if table_key not in ['pin','geo','user']:
        raise ValueError(f'`{table_key}` is not a valid table key.')
    if table_key == 'pin':
        name = 'pinterest'
    elif table_key == 'geo':
        name = 'geolocation'
    elif table_key == 'user':
        name = 'user'
    string = text(f"SELECT * FROM {name}_data LIMIT {row_number}, 1")
    selected_row = connection.execute(string)
    for row in selected_row:
        result = dict(row._mapping)
        result = {key: convert_datetime(val) for key,val in result.items()}
    return result
    # except ValueError as e:
    #     print(e)

def send_to_kafka(
        new_post:dict,
        topic:str,
        invoke_url:str=YAMLReader('constants')['KAFKA_URL']
)->str:
    '''
    Sends posting data to Apache Kafka for processing using a provided
    invoke URL and topic name.  By default, it will look for a `.yaml`
    file `constants` in which the invoke URL is stored as 'KAFKA_URL'.
    '''

    def create_kafka_payload(dict_post)->Response:
        '''
        Subfunction that creates API payload in format
        expected by Apache Kafka.
        '''
        kafka_payload = json.dumps(
            {
                "records": [
                    {
                        "value": dict_post
                    }
                ]
            }
        )
        return kafka_payload

    payload = create_kafka_payload(new_post)
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request(
        "POST",
        invoke_url+f'.{topic}',
        headers=headers,
        data=payload
    )
    return response

def send_to_kinesis(
        new_post:dict,
        partition_key:str,
        invoke_url:str=YAMLReader('constants')['KINESIS_URL']
)->str:
    '''
    Sends posting data to AWS Kkinesis for processing using a provided
    invoke URL and partition key.  By default, it will look for a `.yaml`
    file `constants` in which the invoke URL is stored as 'KINESIS_URL'.

    Function is set up so that the partition key corresponds to the 'topic',
    e.g. `geo`.
    '''
    def create_kinesis_payload(dict_post, partition_key)->Response:
        '''
        Subfunction that creates API payload in format
        expected by Amazon Kinesis.
        '''
        kinesis_payload = json.dumps(
            {
                "Data": dict_post,
                "PartitionKey": f"partition-{partition_key}"
            }
        )
        return kinesis_payload

    payload = create_kinesis_payload(new_post, partition_key)
    headers = {'Content-Type': 'application/json'}
    response = requests.request(
        "PUT",
        invoke_url.format(partition_key),
        headers = headers,
        data = payload
    )
    return response

class AWSDBConnector:
    '''An object which connects to a remote AWS database.'''
    def __init__(self):
        pass
    
    def create_db_connector(self, credentials:str='db_creds')->Engine:
        '''
        Creates connection engine with given database credentials.  
        By default, it looks for a `.yaml` file named `db_creds`.
        '''
        if '.yaml' in credentials:
            credentials = credentials.split('.')[0]

        self.HOST = YAMLReader(credentials)['HOST']
        self.USER = YAMLReader(credentials)['USER']
        self.PASSWORD = YAMLReader(credentials)['PASSWORD']
        self.DATABASE = YAMLReader(credentials)['DATABASE']
        self.PORT = 3306

        engine = sqlalchemy.create_engine(
            f'mysql+pymysql'
            f'://{self.USER}'
            f':{self.PASSWORD}'
            f'@{self.HOST}'
            f':{self.PORT}'
            f'/{self.DATABASE}'
            '?charset=utf8mb4'
        )
        return engine