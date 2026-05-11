from kafka import KafkaProducer
import json
import csv
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('data/input_file/sample.csv') as f:
    reader = csv.DictReader(f)

    for row in reader:
        producer.send('transactions', row)
        print("Sent:", row)
        time.sleep(1)  # simulate real-time

producer.flush()