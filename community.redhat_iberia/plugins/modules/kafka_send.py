#!/usr/bin/python
#
# Copyright (c) 2025, Ramon Gordillo <rgordill@redhat.com>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations
import ssl
import os
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend

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
            - Each header must be a dictionary with exactly one key-value pair.
            - Header values can be strings (will be UTF-8 encoded) or raw bytes.
            - Example format: [{"header1": "value1"}, {"header2": "value2"}]
        required: false
        type: list
        elements: dict
    message:
        description:
            - The message to send to the Kafka topic.
            - Can be either a string (will be UTF-8 encoded) or raw bytes.
            - For Avro messages, pass the binary data directly from the to_avro filter.
            - For text messages, you can pass them directly as strings.
            - For other binary formats, pass the raw bytes.
        required: true
        type: raw
    bootstrap_servers:
        description:
            - A comma-separated list of Kafka bootstrap servers.
        required: true
        type: str
    security_protocol:
        description:
            - Protocol used to communicate with brokers.
            - Valid values are PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
        required: false
        type: str
        default: PLAINTEXT
    sasl_mechanism:
        description:
            - Authentication mechanism when security_protocol is SASL_PLAINTEXT or SASL_SSL.
            - Valid values are PLAIN, GSSAPI, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER.
        required: false
        type: str
    sasl_username:
        description:
            - Username for SASL authentication.
        required: false
        type: str
    sasl_password:
        description:
            - Password for SASL authentication.
        required: false
        type: str
    ssl_cafile:
        description:
            - Path to the CA certificate file for SSL connections.
        required: false
        type: str
    ssl_certfile:
        description:
            - Path to the client certificate file for SSL connections.
        required: false
        type: str
    ssl_keyfile:
        description:
            - Path to the client key file for SSL connections.
        required: false
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

    - name: Send message using SASL/SCRAM authentication
      kafka_send:
        topic: my_topic
        message: "Secure message"
        bootstrap_servers: kafka.example.com:9092
        security_protocol: SASL_SSL
        sasl_mechanism: SCRAM-SHA-512
        sasl_username: myuser
        sasl_password: mypassword
        ssl_cafile: /path/to/ca.pem

    - name: Send message using SSL/TLS
      kafka_send:
        topic: my_topic
        message: "TLS secured message"
        bootstrap_servers: kafka.example.com:9093
        security_protocol: SSL
        ssl_cafile: /path/to/ca.pem
        ssl_certfile: /path/to/client-cert.pem
        ssl_keyfile: /path/to/client-key.pem
"""

from ansible.module_utils.basic import AnsibleModule
from kafka import KafkaProducer
from kafka.errors import (
    KafkaError,
    KafkaTimeoutError,
    NoBrokersAvailable,
    NodeNotReadyError,
    TopicAuthorizationFailedError,
    SecurityDisabledError,
    AuthenticationFailedError,
)

def validate_certificates(module, ssl_config):
    """Validate SSL certificates and keys"""

    def _fail(msg):
        module.fail_json(status='error', error_type='certificate_error', msg=msg)

    def _ensure_file_exists(path, desc):
        if not os.path.isfile(path):
            _fail(f"{desc} file not found: {path}")

    def _load_x509(path, desc):
        _ensure_file_exists(path, desc)
        try:
            with open(path, 'rb') as f:
                load_pem_x509_certificate(f.read(), default_backend())
        except ValueError as e:
            _fail(f"Invalid {desc} format in {path}: {str(e)}")

    def _load_private_key(path):
        _ensure_file_exists(path, 'Private key')
        try:
            with open(path, 'rb') as f:
                load_pem_private_key(f.read(), password=None, backend=default_backend())
        except (ValueError, TypeError) as e:
            _fail(f"Invalid private key format in {path}: {str(e)}")
        except PermissionError as e:
            _fail(f"Permission denied reading private key file {path}: {str(e)}")

    try:
        if 'ssl_cafile' in ssl_config:
            _load_x509(ssl_config['ssl_cafile'], 'CA certificate')

        if 'ssl_certfile' in ssl_config:
            _load_x509(ssl_config['ssl_certfile'], 'Client certificate')

        if 'ssl_keyfile' in ssl_config:
            _load_private_key(ssl_config['ssl_keyfile'])

    except Exception as e:
        module.fail_json(
            status='error',
            error_type='certificate_error',
            msg=f"Error validating SSL certificates: {str(e)}"
        )

def format_kafka_headers(headers):
    """
    Convert Ansible-style headers to Kafka format (list of (str, bytes) tuples)
    
    Args:
        headers: List of single key-value pair dictionaries
        
    Returns:
        List of (str, bytes) tuples or None if headers is None
        
    Raises:
        ValueError: If headers format is invalid
    """
    if not headers:
        return None
        
    formatted_headers = []
    try:
        for header in headers:
            if not isinstance(header, dict) or len(header) != 1:
                raise ValueError("Each header must be a dictionary with exactly one key-value pair")
            
            # Get the single key-value pair from the dictionary
            for key, value in header.items():
                # Convert header value to bytes if it's a string
                header_value = value.encode('utf-8') if isinstance(value, str) else value
                if not isinstance(header_value, bytes):
                    raise ValueError(f"Header value for '{key}' must be either a string or bytes")
                formatted_headers.append((str(key), header_value))
        return formatted_headers
    except Exception as e:
        raise ValueError(f"Invalid headers format: {str(e)}")

def send_message(producer, topic, key, message, headers=None):
    try:
        # Encode key to bytes if it's not None
        key_bytes = key.encode('utf-8') if key is not None else None

        # Convert message to bytes if it's a string
        if isinstance(message, str):
            message = message.encode('utf-8')
        elif not isinstance(message, bytes):
            raise ValueError("Message must be either a string or bytes.")

        # Format headers into Kafka's required format
        formatted_headers = format_kafka_headers(headers)
        
        # Message and headers are now guaranteed to be in the correct format
        future = producer.send(topic, key=key_bytes, value=message, headers=formatted_headers)
        result = future.get(timeout=10)
        return {
            'status': 'success',
            'failed': False,
            'changed': True,
            'topic': topic,
            'partition': result.partition,
            'offset': result.offset,
            'timestamp': result.timestamp,
            'message_details': {
                'has_key': key is not None,
                'has_headers': headers is not None,
                'message_size': len(message)
            }
        }

    except NoBrokersAvailable as e:
        return {
            'failed': True,
            'status': 'error',
            'error_type': 'broker_unavailable',
            'message': f"No brokers available. Check if the bootstrap servers are correct and running: {str(e)}"
        }
    except KafkaTimeoutError as e:
        return {
            'failed': True,
            'status': 'error',
            'error_type': 'timeout',
            'message': f"Operation timed out. The broker might be overloaded or unreachable: {str(e)}"
        }
    except NodeNotReadyError as e:
        return {
            'failed': True,
            'status': 'error',
            'error_type': 'node_not_ready',
            'message': f"The broker node is not ready to handle requests: {str(e)}"
        }
    except TopicAuthorizationFailedError as e:
        return {
            'failed': True,
            'status': 'error',
            'error_type': 'authorization_failed',
            'message': f"Not authorized to access this topic: {str(e)}"
        }
    except (SecurityDisabledError, AuthenticationFailedError) as e:
        return {
            'failed': True,
            'status': 'error',
            'error_type': 'authentication_failed',
            'message': f"Authentication failed. Check your SASL credentials: {str(e)}"
        }
    except ssl.SSLError as e:
        return {
            'failed': True,
            'status': 'error',
            'error_type': 'ssl_error',
            'message': f"SSL certificate error. Check your SSL configuration and certificates: {str(e)}"
        }
    except ValueError as e:
        return {
            'failed': True,
            'status': 'error',
            'error_type': 'value_error',
            'message': str(e)
        }
    except KafkaError as e:
        return {
            'failed': True,
            'status': 'error',
            'error_type': 'kafka_error',
            'message': f"Unexpected Kafka error: {str(e)}"
        }
    # except Exception as e:
    #     return {
    #         'failed': True,
    #         'status': 'error',
    #         'error_type': 'unknown_error',
    #         'message': f"An unexpected error occurred: {str(e)}"
    #     }

def main():

    module = AnsibleModule(
        argument_spec={
        'topic': {'type': 'str', 'required': True},
        'key': {'type': 'str', 'required': False},
        'headers': {'type': 'list', 'required': False, 'default': None},
        'message': {'type': 'raw', 'required': True},
        'bootstrap_servers': {'type': 'str', 'required': True},
        'security_protocol': {'type': 'str', 'required': False, 'default': 'PLAINTEXT'},
        'sasl_mechanism': {'type': 'str', 'required': False},
        'sasl_username': {'type': 'str', 'required': False},
        'sasl_password': {'type': 'str', 'required': False, 'no_log': True},
        'ssl_cafile': {'type': 'str', 'required': False},
        'ssl_certfile': {'type': 'str', 'required': False},
        'ssl_keyfile': {'type': 'str', 'required': False},
        }, 
        supports_check_mode=True)

    # Extract all parameters
    topic = module.params['topic']
    bootstrap_servers = module.params['bootstrap_servers']
    key = module.params.get('key')
    headers = module.params.get('headers')
    message = module.params.get('message')
    
    # Security parameters
    security_protocol = module.params['security_protocol']
    sasl_mechanism = module.params.get('sasl_mechanism')
    sasl_username = module.params.get('sasl_username')
    sasl_password = module.params.get('sasl_password')
    ssl_cafile = module.params.get('ssl_cafile')
    ssl_certfile = module.params.get('ssl_certfile')
    ssl_keyfile = module.params.get('ssl_keyfile')

    # Build configuration for KafkaProducer
    producer_config = {
        'bootstrap_servers': bootstrap_servers
    }

    # Add security protocol if specified
    if security_protocol != 'PLAINTEXT':
        producer_config['security_protocol'] = security_protocol

    # Add SASL configuration if specified
    if security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
        sasl_params = [sasl_mechanism, sasl_username, sasl_password]
        if any(sasl_params) and not all(sasl_params):
            module.fail_json(msg='When configuring SASL, all of sasl_mechanism, sasl_username, and sasl_password must be provided')
        if all(sasl_params):
            producer_config.update({
                'sasl_mechanism': sasl_mechanism,
                'sasl_plain_username': sasl_username,
                'sasl_plain_password': sasl_password
            })

    # Add SSL configuration if specified
    if security_protocol in ['SSL', 'SASL_SSL']:
        ssl_params = {
            'ssl_cafile': ssl_cafile,
            'ssl_certfile': ssl_certfile,
            'ssl_keyfile': ssl_keyfile
        }
        # Only add SSL parameters that were actually specified
        ssl_config = {k: v for k, v in ssl_params.items() if v is not None}
        if ssl_config:
            # Validate SSL certificates before proceeding
            validate_certificates(module, ssl_config)
            producer_config.update(ssl_config)

    try:
        producer = KafkaProducer(**producer_config)
        try:
            # Message encoding moved to send_message function
            result = send_message(producer, topic, key, message, headers)
        finally:
            # Ensure producer is closed even if send_message raises an exception
            producer.close()
        
        module.exit_json(**result)

    except NoBrokersAvailable as e:
        module.fail_json(
            status='error',
            error_type='broker_unavailable',
            msg=f"Failed to connect to bootstrap servers. Check if the servers are running and accessible: {str(e)}"
        )
    except (SecurityDisabledError, AuthenticationFailedError) as e:
        module.fail_json(
            status='error',
            error_type='authentication_failed',
            msg=f"Failed to authenticate with the Kafka brokers. Check your security configuration: {str(e)}"
        )
    except KafkaError as e:
        module.fail_json(
            status='error',
            error_type='connection_error',
            msg=f"Failed to establish connection with Kafka: {str(e)}"
        )
    except Exception as e:
        module.fail_json(
            status='error',
            error_type='unknown_error',
            msg=f"An unexpected error occurred while connecting to Kafka: {str(e)}"
        )

if __name__ == '__main__':
    main()