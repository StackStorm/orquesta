# Copyright 2019 Extreme Networks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from orquesta.specs import loader as spec_loader
from orquesta.specs import mistral as mistral_specs
from orquesta.specs.mistral import v2 as mistral_v2_specs


class SpecTest(unittest.TestCase):

    def setUp(self):
        super(SpecTest, self).setUp()
        self.spec_module_name = 'mistral'

    def test_get_module(self):
        self.assertEqual(
            spec_loader.get_spec_module(self.spec_module_name),
            mistral_specs
        )

    def test_get_spec(self):
        spec_module = spec_loader.get_spec_module(self.spec_module_name)

        self.assertEqual(
            spec_module.WorkflowSpec,
            mistral_specs.WorkflowSpec
        )

    def test_spec_catalog(self):
        spec_module = spec_loader.get_spec_module(self.spec_module_name)

        self.assertEqual(
            spec_module.WorkflowSpec.get_catalog(),
            self.spec_module_name
        )

    def test_spec_version(self):
        self.assertEqual('2.0', mistral_v2_specs.VERSION)
        self.assertEqual('2.0', mistral_specs.VERSION)

    def test_workflow_spec_imports(self):
        self.assertEqual(
            mistral_specs.WorkflowSpec,
            mistral_v2_specs.workflows.WorkflowSpec
        )

    def test_task_spec_imports(self):
        self.assertEqual(
            mistral_specs.TaskDefaultsSpec,
            mistral_v2_specs.tasks.TaskDefaultsSpec
        )

        self.assertEqual(
            mistral_specs.TaskSpec,
            mistral_v2_specs.tasks.TaskSpec
        )
