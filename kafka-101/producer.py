#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=W0102,W0718,C0301,E0712
# fmt: on

""" Kafka Producer  """

from confluent_kafka import Producer

def kafka_producer(broker, topic):
    # Configuration for the Kafka Producer
    producer_config = {
        'bootstrap.servers': broker,
    }

    # Create the Kafka Producer instance
    producer = Producer(producer_config)

    def publish_message(message):
        # Convert the message to bytes (Kafka requires bytes)
        message_bytes = message.encode('utf-8')
        
        # Publish the message to the topic
        producer.produce(topic, message_bytes)
        
        # Flush the messages to ensure they are sent to Kafka
        producer.flush()

    return publish_message

# Example usage:
if __name__ == '__main__':
    broker = 'localhost:9092'
    topic = 'test_topic'
    
    # Create the Kafka producer
    publish_to_kafka = kafka_producer(broker, topic)
    
    # Publish a sample message
    message = "Hello, Kafka!"
    publish_to_kafka(message)
