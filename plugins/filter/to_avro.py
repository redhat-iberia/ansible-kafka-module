#
# Copyright (c) 2025, Ramon Gordillo <rgordill@redhat.com>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

DOCUMENTATION = r"""
---
name: to_avro
version_added: "1.0.0"
collection: redhat_iberia.kafka
short_description: Convert Python objects to Avro binary format
description:
  - This filter plugin provides conversion between Python objects and Avro binary format
  - Implements the `to_avro` filter for encoding (paired with `message_from_avro` for decoding)
  - Uses the `fastavro` library for high-performance Avro operations
author: Ramon Gordillo (@rgordill)
options:
  _input:
    description: The input value to convert
    type: raw
    required: true
  schema:
    description: Avro schema definition as a dict or JSON string
    type: raw
    required: true
"""

EXAMPLES = r"""
- name: Convert user data to Avro
  vars:
    user_schema:
      type: "record"
      name: "User"
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
      ]
    user_data:
      name: "John Doe"
      age: 30
  set_fact:
    avro_binary: "{{ user_data | to_avro(user_schema) }}"

- name: Decode Avro data
  set_fact:
    decoded_data: "{{ avro_binary | message_from_avro(user_schema) }}"
"""

RETURN = r"""
_value:
  description: The converted data
  type: raw
  returned: always
"""

from ansible.errors import AnsibleFilterError
import fastavro
import io
import json


def to_avro(obj, schema, schema_id=None, schemaless=True):
    """
    Convert a Python object to Avro binary format using the provided schema.

    Args:
      obj: The Python object to convert
      schema: Avro schema definition as a dictionary
      schema_id: Optional schema ID to include
      schemaless: Whether to use schemaless writer (default: True)

    Returns:
      bytes: Avro binary encoded data

    Raises:
      AnsibleFilterError: If there's an error during conversion
    """
    try:
        # Convert string inputs to dict if needed
        if isinstance(schema, str):
            try:
                schema = json.loads(schema)
            except json.JSONDecodeError as e:
                raise AnsibleFilterError(f"Invalid JSON in schema: {str(e)}")

        if isinstance(obj, str):
            try:
                obj = json.loads(obj)
            except json.JSONDecodeError as e:
                raise AnsibleFilterError(f"Invalid JSON in data: {str(e)}")

        # Parse the Avro schema
        schema = fastavro.parse_schema(schema)

        # Create a binary stream to write the data
        bytes_writer = io.BytesIO()

        if schema_id is not None:
            # Prepend schema ID to the binary data if provided
            bytes_writer.write(b"\x00")  # Magic byte
            bytes_writer.write(schema_id.to_bytes(4, byteorder="big"))

        # Write the object using fastavro
        if schemaless:
            fastavro.schemaless_writer(bytes_writer, schema, obj)
        else:
            fastavro.writer(bytes_writer, schema, [obj])

        # Get the binary data
        avro_bytes = bytes_writer.getvalue()
        return avro_bytes

    except Exception as e:
        raise AnsibleFilterError(f"Error converting to Avro: {str(e)}")


class FilterModule:
    """Ansible filter for Avro encoding operations"""

    def filters(self):
        return {"to_avro": to_avro}
