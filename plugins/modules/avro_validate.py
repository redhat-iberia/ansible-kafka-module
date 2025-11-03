#!/usr/bin/python

#
# Copyright (c) 2025, Ramon Gordillo <rgordill@redhat.com>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

DOCUMENTATION = r"""
---
module: avro_validate
short_description: Validate data against an Avro schema
description:
    - This module validates JSON data against an Avro schema using fastavro
options:
    schema:
        description:
            - The Avro schema to validate against (as a dictionary or string)
        required: true
        type: raw
    data:
        description:
            - The data to validate (as a dictionary or string)
        required: true
        type: raw
requirements:
    - fastavro
author:
    - Your Name (@yourgithub)
"""

EXAMPLES = r"""
- name: Validate data against schema
  avro_validate:
    schema:
      type: "record"
      name: "User"
      fields:
        - {name: "name", type: "string"}
        - {name: "age", type: "int"}
    data:
      name: "John Doe"
      age: 30
"""

RETURN = r"""
msg:
    description: The validation result message
    type: str
    returned: always
valid:
    description: Whether the data is valid according to the schema
    type: bool
    returned: always
"""

from ansible.module_utils.basic import AnsibleModule
import json

try:
    from fastavro import parse_schema, validate
    HAS_FASTAVRO = True
except ImportError:
    HAS_FASTAVRO = False

def run_module():

    result = {
        'changed': False,
        'valid': False,
        'msg': ''
    }

    module = AnsibleModule(
        argument_spec={
            'schema': {'type': 'raw', 'required': True},
            'data': {'type': 'raw', 'required': True}
        },
        supports_check_mode=True
    )

    if not HAS_FASTAVRO:
        module.fail_json(msg='The fastavro module is required', **result)

    schema = module.params['schema']
    data = module.params['data']

    # Convert string inputs to dict if needed
    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except json.JSONDecodeError:
            module.fail_json(msg='Invalid JSON in schema', **result)

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            module.fail_json(msg='Invalid JSON in data', **result)

    try:
        parsed_schema = parse_schema(schema) # pyright: ignore[reportPossiblyUnboundVariable]
        validate(data, parsed_schema)        # pyright: ignore[reportPossiblyUnboundVariable]
        result['valid'] = True
        result['msg'] = 'Data is valid according to the schema'
    except Exception as e:
        result['msg'] = f'Validation failed: {str(e)}'
        module.fail_json(**result)

    module.exit_json(**result)

def main():
    run_module()

if __name__ == '__main__':
    main()
