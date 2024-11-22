kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}

MY_NAME = "msolonin"
SENSORS_TOPIC_NAME = f'building_sensors_{MY_NAME}'
TEMPERATURE_ALERT_TOPIC_NAME = f'temperature_alerts_{MY_NAME}'
HUMIDITY_ALERT_TOPIC_NAME = f'humidity_alerts_{MY_NAME}'
TOPICS = [SENSORS_TOPIC_NAME, TEMPERATURE_ALERT_TOPIC_NAME, HUMIDITY_ALERT_TOPIC_NAME]
TIME_SLEEP = 2
NUM_MESSAGES = 20
