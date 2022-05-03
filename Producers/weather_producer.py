from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "type": "weather",
        "temperature": random.randint(20, 40),
        "condition": random.choice(["sunny", "rainy", "cloudy"]),
        "timestamp": time.time()
    }

    producer.send('iot-topic', value=data)
    print(data)

    time.sleep(4)