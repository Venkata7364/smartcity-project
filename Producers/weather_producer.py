from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "type": "weather",
        "temperature": random.randint(20, 40),
        "humidity": random.randint(30, 80),
        "city": "Houston",
        "timestamp": time.time()
    }

    producer.send('iot-topic', value=data)
    print("Sent:", data)

    time.sleep(2)
