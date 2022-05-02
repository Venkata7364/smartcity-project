from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="smartcity",
    user="admin",
    password="admin"
)

cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS vehicle_data (
    vehicle_id INT,
    speed INT,
    location VARCHAR(10),
    timestamp DOUBLE PRECISION
)
""")
conn.commit()

# Kafka Consumer
consumer = KafkaConsumer(
    'smartcity-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started...")

for message in consumer:
    data = message.value
    print("Received:", data)

    cursor.execute("""
        INSERT INTO vehicle_data (vehicle_id, speed, location, timestamp)
        VALUES (%s, %s, %s, %s)
    """, (
        data['vehicle_id'],
        data['speed'],
        data['location'],
        data['timestamp']
    ))

    conn.commit()
