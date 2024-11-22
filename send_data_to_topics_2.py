from kafka import KafkaProducer
from configs import kafka_config, SENSORS_TOPIC_NAME, TIME_SLEEP, NUM_MESSAGES
import json
import uuid
import time
import random

SENSOR_UID = f"SENSOR_[{uuid.uuid4()}]"
print(f"Sensor ID: {SENSOR_UID}")
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)


for i in range(NUM_MESSAGES):
    try:
        data = {
            "timestamp": time.time(),
            "id": SENSOR_UID,
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85)
        }
        producer.send(SENSORS_TOPIC_NAME, key=str(uuid.uuid4()), value=data)
        producer.flush()
        print(f"Message {i} sent to topic {SENSORS_TOPIC_NAME}: {data} successfully.")
        time.sleep(TIME_SLEEP)
    except Exception as e:
        print(f"An error occurred: {e}")
producer.close()
