import json
from datetime import datetime, timedelta

import requests
from kafka import KafkaProducer

# Set Kafka config
kafka_broker_hostname = 'localhost'
kafka_broker_port_no = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_broker_port_no
kafka_topic = 'aq-raw-data'

producer = KafkaProducer(bootstrap_servers=[kafka_broker], api_version=(2, 6, 0))
print("Connected to kafka broker on server")

past_hour = (datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
total = 10001
page = 1
limit = 10000
while (page * limit) < total:
    params = {'limit': limit, 'page': page, 'date_from': past_hour}
    response = requests.get("https://api.openaq.org/v1/measurements", params=params)
    if response.status_code == 200:
        data = response.json()
        total = data['meta']['found']
        print('Total: ', total)
        results = data['results']
        print('Fetched: ', len(results))
        for row in results:
            producer.send(kafka_topic, bytes(str.encode(json.dumps(row))))
    page += 1
