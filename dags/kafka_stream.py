from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid
from confluent_kafka import Producer
import requests

default_args = {
    'owner': 'sahil',
    'start_date': datetime(2024, 12, 10, 22, 51),
    'retries': 3,  # Added retry policy
    'retry_delay': timedelta(minutes=5)  # Added delay between retries
}

def get_data():
    res = requests.get('https://randomuser.me/api/')
    res.raise_for_status()  # Raise an error for bad responses
    return res.json()['results'][0]

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data



def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    # Configuration for Confluent Kafka Producer
    producer_config = {
        'bootstrap.servers': 'broker:29092',  # Ensure this matches your Kafka broker configuration
        'client.id': 'python-producer'
    }

    # Configuration for KafkaProducer (for continuous streaming)
    kafka_producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    producer = Producer(producer_config)
    topic = 'users_created'

    curr_time = time.time()

    while True:
        # Break the loop after 1 minute
        if time.time() > curr_time + 60:
            break
        try:
            # Get and format the data
            res = get_data()
            formatted_data = format_data(res)

            # Serialize data to JSON format
            message = json.dumps(formatted_data).encode('utf-8')

            # Produce the message using Confluent Kafka Producer
            producer.produce(topic, value=message)

            # Produce the message using KafkaProducer
            kafka_producer.send(topic, message)

            # Ensure all messages are sent for Confluent Kafka Producer
            producer.flush()

        except requests.RequestException as e:
            logging.error(f"Request failed: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue

with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
