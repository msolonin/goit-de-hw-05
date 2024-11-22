from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, TOPICS


NUM_PARTITIONS = 2
REPLICATION_FOLDER = 1


admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

for topic_name in TOPICS:
    new_topic = NewTopic(name=topic_name, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FOLDER)
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


print(admin_client.list_topics())
admin_client.close()
