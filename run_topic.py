from configparser import ConfigParser
from argparse import ArgumentParser, FileType
from confluent_kafka.admin import AdminClient, NewTopic

parser = ArgumentParser()
parser.add_argument('config_file', type=FileType('r'))
parser.add_argument('new_topic')
args = parser.parse_args()

config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])

a = AdminClient(config)

new_topics = [NewTopic(topic, num_partitions=3, replication_factor=3) for topic in [args.new_topic]]
# Note: In a multi-cluster production scenario, it is more typical to use a replication_factor of 3 for durability.

# Call create_topics to asynchronously create topics. A dict
# of <topic,future> is returned.
fs = a.create_topics(new_topics)

# Wait for each operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))