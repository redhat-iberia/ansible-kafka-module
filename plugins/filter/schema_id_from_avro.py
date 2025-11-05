#
# Copyright (c) 2025, Ramon Gordillo <rgordill@redhat.com>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

DOCUMENTATION = r"""
---
name: schema_id_from_avro
version_added: "1.0.0"
collection: redhat_iberia.kafka
short_description: Extract embedded schema id from Avro binary (Confluent wire format)
description:
  - Reads the Confluent Avro wire format header (1-byte magic + 4-byte schema id) and returns the schema id.
  - Useful when Avro payloads include schema registry ids.
author: Ramon Gordillo (@rgordill)
options:
  _input:
    description: The Avro binary payload or a bytes/string representation
    type: raw
    required: true
"""

EXAMPLES = r"""
- name: Get schema id from Avro payload
  set_fact:
    schema_id: "{{ avro_payload | schema_id_from_avro }}"
"""

RETURN = r"""
_value:
  description: Schema id extracted from the Avro payload
  type: str
  returned: always
"""

from ansible.errors import AnsibleFilterError
import io


def schema_id_from_avro(avro_bytes):
    """
    Extract the Avro schema id from Avro binary data. Only works for schemaless Avro data.

    Args:
      avro_bytes: The Avro binary data

    Returns:
      string: Avro schema id

    Raises:
      AnsibleFilterError: If there's an error during extraction
    """

    try:
        # Convert input to bytes if it's a string or AnsibleUnsafeText
        if not isinstance(avro_bytes, bytes):
            avro_bytes = str(avro_bytes).encode("utf-8")

        # Create a binary stream with the input data
        bytes_reader = io.BytesIO(avro_bytes)

        magic_byte = bytes_reader.read(1)
        if magic_byte != b"\x00":
            raise AnsibleFilterError("Invalid magic byte in Avro data")
        schema_id = int.from_bytes(bytes_reader.read(4), byteorder="big")
        return str(schema_id)

    except Exception as e:
        raise AnsibleFilterError(f"Error extracting schema id from Avro: {str(e)}")


class FilterModule:
    """Ansible filter for extracting Avro schema IDs"""

    def filters(self):
        return {"schema_id_from_avro": schema_id_from_avro}
