# Copyright (c) 2025, Ramon Gordillo <rgordill@redhat.com>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from unittest.mock import Mock, patch

from ansible.module_utils.basic import AnsibleModule
from ansible_collections.community.internal_test_tools.tests.unit.plugins.modules.utils import (
    ModuleTestCase,
    set_module_args,
)

from ansible_collections.redhat_iberia.kafka.plugins.modules.avro_generate import run_module


class TestAvroGenerate(ModuleTestCase):
    def setUp(self):
        super().setUp()
        self.module = run_module()

    def test_avro_generate(self):
        set_module_args(
            {
                "schema": {
                    "type": "record",
                    "name": "User",
                    "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}],
                },
                "num_samples": 1,
            }
        )
        result = self.module.run()

        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertIn("name", result["data"][0])
        self.assertIn("age", result["data"][0])

    def test_avro_generate_multiple_samples(self):
        set_module_args(
            {
                "schema": {
                    "type": "record",
                    "name": "User",
                    "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}],
                },
                "num_samples": 5,
            }
        )
        result = self.module.run()

        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 5)
        for record in result["data"]:
            self.assertIn("name", record)
            self.assertIn("age", record)
