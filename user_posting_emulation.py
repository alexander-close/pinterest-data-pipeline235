import random
from AWSDBConnector import AWSDBConnector
from time import sleep
from typing import Callable
from utils import generate_post, send_to_kafka, send_to_kinesis

new_connector = AWSDBConnector()

def run_infinite_post_data_loop(
    send_to_stream:Callable,
    random_seed:int=42,
    table_keys:list=['pin','geo','user']
):
    '''
    Emulates continuous stream of posting data using an infinite loop.
    the user is provide the destination processor in the form of either
    of the functions `send_to_kafka` or `send_to_kinesis`.  The user may
    also specify a list of 'table keys' (defaulted to `[pin,geo,user]` and
    a random seed choice for reproducibility.)

    The infinite loop contains a sub-loop which iterates over the table keys,
    generates the respective posting data, packages them into the appropriate
    payload and sends them to the streaming processor.

    The loop also prints outs the pre-processed "post" to terminal console.

    * NB:  Comment out the response line when not connecting to Kafka/Kinesis. *
    '''
    random.seed(random_seed)
    while True:
        sleep(random.randrange(0, 2))
        random_row_num = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            for table_key in table_keys:
                new_post = generate_post(
                    table_key, random_row_num, connection
                )
                # response = send_to_stream(new_post,table_key)

                print('='*30)
                print(new_post)
                print()
                # print(f'{table_key} status code:', response.status_code)

if __name__ == "__main__":
    print('User post stream ongoing...')
    run_infinite_post_data_loop(
        # COMMENT OUT AS NEEDED
        send_to_kafka
        # send_to_kinesis
    )