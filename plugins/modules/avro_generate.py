#!/usr/bin/python

#
# Copyright (c) 2025, Ramon Gordillo <rgordill@redhat.com>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

DOCUMENTATION = r"""
---
module: avro_generate
short_description: Generate sample data from an Avro schema
description:
    - This module generates sample data that conforms to a given Avro schema using fastavro
options:
    schema:
        description:
            - The Avro schema to use for generating samples (as a dictionary or string)
        required: true
        type: raw
    num_samples:
        description:
            - Number of samples to generate
        required: false
        type: int
        default: 1
requirements:
    - fastavro
author:
    - Your Name (@yourgithub)
"""

EXAMPLES = r"""
- name: Generate one sample from schema
  avro_generate:
    schema:
      type: "record"
      name: "User"
      fields:
        - name: "name"
          type: "string"
        - name: "age"
          type: "int"

- name: Generate multiple samples from schema
  avro_generate:
    schema:
      type: "record"
      name: "User"
      fields:
        - {name: "name", type: "string"}
        - {name: "age", type: "int"}
    num_samples: 3
"""

RETURN = r"""
samples:
    description: List of generated samples conforming to the schema
    type: list
    returned: always
msg:
    description: Status message
    type: str
    returned: always
"""

from ansible.module_utils.basic import AnsibleModule

import json

try:
    from fastavro import parse_schema
    from fastavro.utils import generate_many
    HAS_FASTAVRO = True
except ImportError:
    HAS_FASTAVRO = False

def run_module():

    result = {
        'changed': False,
        'samples': [],
        'msg': ''
    }

    module = AnsibleModule(
        argument_spec={
            'schema': {'type': 'raw', 'required': True},
            'num_samples': {'type': 'int', 'required': False, 'default': 1}
        },
        supports_check_mode=True
    )

    if not HAS_FASTAVRO:
        module.fail_json(msg='The fastavro module is required', **result)

    schema = module.params['schema']
    num_samples = module.params['num_samples']

    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except json.JSONDecodeError:
            module.fail_json(msg='Invalid JSON in schema', **result)

    try:
        # Validate schema first
        parse_schema(schema)                               # pyright: ignore[reportPossiblyUnboundVariable]
        samples = list(generate_many(schema, num_samples)) # pyright: ignore[reportPossiblyUnboundVariable]
        result['samples'] = samples
        result['msg'] = f'Successfully generated {num_samples} sample(s)'
    except Exception as e:
        module.fail_json(msg=f'Error generating samples: {str(e)}', **result)

    module.exit_json(**result)

def main():
    run_module()

if __name__ == '__main__':
    main()
