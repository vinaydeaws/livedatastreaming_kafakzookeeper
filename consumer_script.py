from kafka import KafkaConsumer
import json
import logging
import time


topic_name = 'test_topic'

consumer = KafkaConsumer(topic_name, bootstrap_servers='localhost:9092', group_id='test-group')

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")

consumer.close()
