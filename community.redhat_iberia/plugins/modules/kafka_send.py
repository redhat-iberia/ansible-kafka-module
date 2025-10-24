#!/usr/bin/python
#
# Copyright (c) 2025, Ramon Gordillo <rgordill@redhat.com>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

DOCUMENTATION = r"""
---
module: kafka_send
short_description: Ansible module for interacting with Kafka topics.
description:
    - This module allows sending and receiving messages from Kafka topics.
    - It uses the KafkaProducer and KafkaConsumer classes from the kafka-python library.
options:
    topic:
        description:
            - The Kafka topic to interact with.
        required: true
        type: str
    key:
        description:
            - The key to be used when sending the message.
            - This is optional and helps with message partitioning.
        required: false
        type: str
    headers:
        description:
            - A list of headers to be sent with the message.
            - Headers are key-value pairs that provide metadata about the message.
        required: false
        type: list
    message:
        description:
            - The message to send to the Kafka topic as raw bytes.
            - For Avro messages, pass the binary data directly from the to_avro filter.
            - For text messages, encode them to bytes before passing to this module.
        required: false
        type: raw
    bootstrap_servers:
        description:
            - A comma-separated list of Kafka bootstrap servers.
        required: true
        type: str
requirements:
    - kafka-python
author:
    - Ramon Gordillo (@rgordill)
examples:
    - name: Send a text message to a Kafka topic
      kafka_send:
        topic: my_topic
        key: my_key
        headers: 
            - header1: value1
            - header2: value2
        message: "Hello, Kafka!"
        bootstrap_servers: localhost:9092

    - name: Send raw bytes to a Kafka topic
      kafka_send:
        topic: my_topic
        key: my_key
        message: !binary |-
          SGVsbG8sIEthZmthIQ==  # "Hello, Kafka!" in base64
        bootstrap_servers: localhost:9092

"""

from ansible.module_utils.basic import AnsibleModule
from kafka import KafkaProducer
from kafka.errors import KafkaError

def send_message(producer, topic, key, message, headers=None):
    try:
        # Encode key to bytes if it's not None
        key_bytes = key.encode('utf-8') if key is not None else None

        # Throw an exception if message is not bytes
        if not isinstance(message, bytes):
            raise ValueError("Message must be of type 'bytes'.")
        
        # Pass the message through as-is since it's already binary data
        future = producer.send(topic, key=key_bytes, value=message, headers=headers)
        result = future.get(timeout=10)
        return {'status': 'success', 'result': result}
    except KafkaError as e:
        return {'status': 'error', 'message': str(e)}
    except ValueError as e:
        return {'status': 'error', 'message': str(e)}

def main():

    module = AnsibleModule(
        argument_spec={
        'topic': {'type': 'str', 'required': True},
        'key': {'type': 'str', 'required': False},
        'headers': {'type': 'list', 'required': False, 'default': None},
        'message': {'type': 'raw', 'required': True},
        'bootstrap_servers': {'type': 'str', 'required': True},
        }, 
        supports_check_mode=True)

    topic = module.params['topic']
    bootstrap_servers = module.params['bootstrap_servers']
    key = module.params.get('key')
    headers = module.params.get('headers')
    message = module.params.get('message')

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    # Message encoding moved to send_message function
    result = send_message(producer, topic, key, message, headers)
    producer.close()

    module.exit_json(**result)

if __name__ == '__main__':
    main()