from kafka import KafkaProducer
import json
import time
import random

# Create producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)

print("Producer started...")

while True:
    data = {
        "vehicle_id": random.randint(1, 100),
        "speed": random.randint(20, 120),
        "location": random.choice(["A", "B", "C"]),
        "timestamp": time.time()
    }

    # Send JSON data
    producer.send(
        'smartcity-topic',
        value=json.dumps(data).encode('utf-8')
    )

    print("Sent:", data)

    time.sleep(2)