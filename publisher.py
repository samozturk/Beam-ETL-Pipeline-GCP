import csv
import time
from google.cloud import pubsub_v1
import os

project = 'samet-sandbox'
pubsub_topic = 'projects/samet-sandbox/topics/movies'
service_account_key = 'samet-sandbox-c4834a547b69.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key
input_file = 'movies.csv'

publisher = pubsub_v1.PublisherClient()
with open('movies.csv', 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(pubsub_topic, row)
        time.sleep(1)