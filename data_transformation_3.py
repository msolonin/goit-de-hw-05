from kafka import KafkaConsumer
from kafka import KafkaProducer
from configs import kafka_config, MY_NAME, SENSORS_TOPIC_NAME, TEMPERATURE_ALERT_TOPIC_NAME, HUMIDITY_ALERT_TOPIC_NAME
import json
import uuid
import sys

try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka producer initialized successfully.")
except Exception as e:
    print(f"Failed to initialize KafkaProducer: {e}")
    sys.exit(1)


try:
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'{MY_NAME}_consumer_group'
    )

    consumer.subscribe([SENSORS_TOPIC_NAME])

    print(f"Subscribed to topic '{SENSORS_TOPIC_NAME}' successfully.")
except Exception as e:
    print(f"Failed to initialize KafkaConsumer or subscribe to topics: {e}")
    sys.exit(1)


try:
    print("Starting to consume messages...")
    for message in consumer:
        try:
            print(
                f"Topic: {message.topic}, "
                f"Partition: {message.partition}, Offset: {message.offset}, "
                f"Key: {message.key}, Value: {message.value}"
            )
            if message.value['temperature'] > 40:
                print(f"Temperature alert: {message.value}")
                producer.send(TEMPERATURE_ALERT_TOPIC_NAME, key=str(uuid.uuid4()), value=message.value)
            if 20 < message.value['humidity'] <= 70:
                print(f"Humidity alert: {message.value}")
                producer.send(HUMIDITY_ALERT_TOPIC_NAME, key=str(uuid.uuid4()), value=message.value)
        except Exception as msg_err:
            print(f"Error processing message: {msg_err}")
except KeyboardInterrupt:
    print("Consumer interrupted by user. Exiting...")
except Exception as e:
    print(f"An error occurred during consumption: {e}")
finally:
    try:
        consumer.close()
        print("Kafka consumer closed successfully.")
    except Exception as e:
        print(f"Error closing Kafka consumer: {e}")
    try:
        producer.close()
        print("Kafka producer closed successfully.")
    except Exception as e:
        print(f"Error closing Kafka producer: {e}")