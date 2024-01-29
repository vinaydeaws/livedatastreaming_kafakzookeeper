from datetime import datetime, timedelta
from kafka import KafkaProducer
import json
import requests
import logging
import time  # Import the time module

producer = KafkaProducer(bootstrap_servers='localhost:9092')

topic_name = 'test_topic'

for i in range(10):
    message = f'Message {i}'
    producer.send(topic_name, value=message.encode('utf-8'))
    print(f'Sent: {message}')
    time.sleep(1)

producer.close()
