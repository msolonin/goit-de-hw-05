from kafka import KafkaConsumer
from configs import kafka_config, MY_NAME, TEMPERATURE_ALERT_TOPIC_NAME, HUMIDITY_ALERT_TOPIC_NAME
import json
import sys

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

    consumer.subscribe([TEMPERATURE_ALERT_TOPIC_NAME, HUMIDITY_ALERT_TOPIC_NAME])

    print(f"Subscribed to topic '{TEMPERATURE_ALERT_TOPIC_NAME, HUMIDITY_ALERT_TOPIC_NAME}' successfully.")
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
            if message.topic == TEMPERATURE_ALERT_TOPIC_NAME:
                print(f"Temperature alert: {message.value}")
            if message.topic == HUMIDITY_ALERT_TOPIC_NAME:
                print(f"Humidity alert: {message.value}")
        except Exception as msg_err:
            print(f"Error processing message: {msg_err}")
except KeyboardInterrupt:
    print("Consumer interrupted by user. Exiting...")
except Exception as e:
    print(f"An error occurred during consumption: {e}")
