#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=W0102,W0718,C0301,E0712
# fmt: on

""" Kafka Consumer  """

from confluent_kafka import Consumer, KafkaError

def kafka_consumer(broker, topic, group_id):
    # Configuration for the Kafka Consumer
    consumer_config = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the earliest message in the topic
    }

    # Create the Kafka Consumer instance
    consumer = Consumer(consumer_config)

    def consume_messages(callback):
        # Subscribe to the topic
        consumer.subscribe([topic])

        while True:
            # Poll for new messages
            message = consumer.poll(1.0)

            if message is None:
                # No new messages received, continue polling
                continue
            if message.error():
                # Handle any errors that occurred while polling
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of partition for topic: {message.topic()}")
                else:
                    print(f"Error while polling Kafka: {message.error()}")
                continue
            
            # Decode the message from bytes to a string
            message_value = message.value().decode('utf-8')
            
            # Process the message using the provided callback function
            callback(message_value)

    return consume_messages

# Example usage:
if __name__ == '__main__':
    broker = 'localhost:9092'
    topic = 'test_topic'
    group_id = 'test_group'
    
    def process_message(message):
        print(f"Received message: {message}")
    
    # Create the Kafka consumer
    consume_from_kafka = kafka_consumer(broker, topic, group_id)
    
    # Start consuming messages
    consume_from_kafka(process_message)
