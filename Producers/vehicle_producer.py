from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "type": "vehicle",
        "vehicle_id": random.randint(1, 100),
        "speed": random.randint(20, 120),
        "timestamp": time.time()
    }

    producer.send('iot-topic', value=data)
    print(data)

    time.sleep(2)