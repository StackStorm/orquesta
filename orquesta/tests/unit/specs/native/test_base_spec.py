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

from orquesta.specs import loader
from orquesta.specs import native as orquesta_specs
from orquesta.specs.native import v1 as orquesta_specs_v1


class SpecTest(unittest.TestCase):

    def setUp(self):
        super(SpecTest, self).setUp()
        self.spec_module_name = 'native'

    def test_get_module(self):
        self.assertEqual(
            loader.get_spec_module(self.spec_module_name),
            orquesta_specs
        )

    def test_get_spec(self):
        spec_module = loader.get_spec_module(self.spec_module_name)

        self.assertEqual(
            spec_module.WorkflowSpec,
            orquesta_specs.WorkflowSpec
        )

    def test_spec_catalog(self):
        spec_module = loader.get_spec_module(self.spec_module_name)

        self.assertEqual(
            spec_module.WorkflowSpec.get_catalog(),
            self.spec_module_name
        )

    def test_spec_version(self):
        self.assertEqual('1.0', orquesta_specs_v1.VERSION)
        self.assertEqual('1.0', orquesta_specs.VERSION)

    def test_workflow_spec_imports(self):
        self.assertEqual(
            orquesta_specs.WorkflowSpec,
            orquesta_specs_v1.models.WorkflowSpec
        )

    def test_task_spec_imports(self):
        self.assertEqual(
            orquesta_specs.TaskTransitionSpec,
            orquesta_specs_v1.models.TaskTransitionSpec
        )

        self.assertEqual(
            orquesta_specs.TaskSpec,
            orquesta_specs_v1.models.TaskSpec
        )
