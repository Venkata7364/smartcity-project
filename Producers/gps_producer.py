from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "type": "gps",
        "latitude": round(random.uniform(10.0, 50.0), 4),
        "longitude": round(random.uniform(10.0, 50.0), 4),
        "timestamp": time.time()
    }

    producer.send('iot-topic', value=data)
    print(data)

    time.sleep(2)