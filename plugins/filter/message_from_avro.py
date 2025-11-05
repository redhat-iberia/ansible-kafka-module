# Copyright (c) 2025, Ramon Gordillo <rgordill@redhat.com>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

DOCUMENTATION = r"""
---
name: message_from_avro
version_added: "1.0.0"
collection: redhat_iberia.kafka
short_description: Convert Avro binary data back to Python objects
description:
  - This filter plugin converts Avro binary data back to Python objects
  - Uses `fastavro` for high-performance Avro operations
  - Companion filter to `to_avro` for bidirectional conversion
author: Ramon Gordillo (@rgordill)
options:
  _input:
    description: The Avro binary data to convert
    type: raw
    required: true
  schema:
    description: Avro schema definition as a dict or JSON string
    type: raw
    required: true
"""

EXAMPLES = r"""
- name: Decode user data from Avro
  vars:
    schema:
      type: "record"
      name: "User"
      fields:
        - {name: "name", type: "string"}
        - {name: "age", type: "int"}
  set_fact:
    decoded_user: "{{ avro_binary | message_from_avro(schema) }}"

- name: Decode user data from Avro with schema id
  vars:
    schema:
      type: "record"
      name: "User"
      fields:
        - {name: "name", type: "string"}
        - {name: "age", type: "int"}
    schema_id: 1234567890
  set_fact:
    decoded_user: "{{ avro_binary | message_from_avro(schema, schema_id) }}"

- name: Round-trip conversion example
  vars:
    data:
      name: "Jane Smith"
      age: 25
    schema:
      type: "record"
      name: "User"
      fields:
        - {name: "name", type: "string"}
        - {name: "age", type: "int"}
    avro_data: "{{ data | to_avro(schema, schema_id) }}"
    decoded_data: "{{ avro_data | message_from_avro(schema, schema_id) }}"
  debug:
    msg: "Decoded data matches original: {{ data == decoded_data }}"
"""

RETURN = r"""
_value:
  description: The decoded Python object
  type: raw
  returned: always
"""

from ansible.errors import AnsibleFilterError
import fastavro
import io
import json


def message_from_avro(avro_bytes, schema, schema_id=None, schemaless=True):
    """
    Convert Avro binary data back to a Python object using the provided schema.

    Args:
      avro_bytes: The Avro binary data to convert
      schema: Avro schema definition as a dictionary
      schema_id: Optional schema ID to validate
      schemaless: Whether to use schemaless reader (default: True)

    Returns:
      object: Python object decoded from Avro

    Raises:
      AnsibleFilterError: If there's an error during conversion
    """
    try:
        # Convert input to bytes if it's a string or AnsibleUnsafeText
        if not isinstance(avro_bytes, bytes):
            avro_bytes = str(avro_bytes).encode("utf-8")

        # Convert string inputs to dict if needed
        if isinstance(schema, str):
            try:
                schema = json.loads(schema)
            except json.JSONDecodeError as e:
                raise AnsibleFilterError(f"Invalid JSON in schema: {str(e)}")

        # Parse the Avro schema
        schema = fastavro.parse_schema(schema)

        # Create a binary stream with the input data
        bytes_reader = io.BytesIO(avro_bytes)

        if schema_id is not None:
            # Read and validate schema ID
            magic_byte = bytes_reader.read(1)
            if magic_byte != b"\x00":
                raise AnsibleFilterError("Invalid magic byte in Avro data")
            read_schema_id = int.from_bytes(bytes_reader.read(4), byteorder="big")
            if str(read_schema_id) != schema_id:
                raise AnsibleFilterError(f"Schema ID mismatch: expected {schema_id}, got {read_schema_id}")

        # Read the object using fastavro
        if schemaless:
            record = fastavro.schemaless_reader(bytes_reader, schema, None)
        else:
            iterator = fastavro.reader(bytes_reader, schema)
            record = next(iterator)

        return record

    except Exception as e:
        raise AnsibleFilterError(f"Error converting from Avro: {str(e)}")


class FilterModule:
    """Ansible filter for Avro extracting message from Avro binary"""

    def filters(self):
        return {"message_from_avro": message_from_avro}
