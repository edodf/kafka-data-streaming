#!/usr/bin/env python
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import time

if __name__ == '__main__':
    # Parse the command line.
    # Menggunakan ArgumentParser untuk membaca argumen baris perintah
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('topic')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    # Membaca konfigurasi dari file yang telah ditentukan
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    # Membuat instance Kafka Producer dengan konfigurasi yang telah dibaca
    producer = Producer(config)

    # Optional per-message delivery callback
    # Digunakan untuk menangani hasil pengiriman pesan (berhasil atau gagal)
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Topic {}, Event msg = {}".format(msg.topic(), msg.value().decode('utf-8')))

    # Read and send data from a CSV file to Kafka topic
    # Membaca dan mengirim data dari file CSV ke topik Kafka
    data = open('/project/hands-on/beach-water-quality-automated-sensors-1.csv','r').read().split("\n")
    for msg in data:
        producer.produce(args.topic, msg, callback=delivery_callback)
        print(msg)
        time.sleep(1)  # Memberikan jeda 1 detik antara pengiriman pesan

    # Block until the messages are sent.
    # Blocking hingga semua pesan dikirim dan terkonfirmasi
    producer.poll(10000)
    producer.flush()
