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

from orchestra import specs
from orchestra.specs import v2 as v2_specs


class ProperSpecTest(unittest.TestCase):

    def test_spec_version(self):
        self.assertEqual('2.0', v2_specs.VERSION)
        self.assertEqual('2.0', specs.VERSION)

    def test_workflow_spec_imports(self):
        self.assertEqual(
            specs.DirectWorkflowSpec,
            v2_specs.workflows.DirectWorkflowSpec
        )

        self.assertEqual(
            specs.ReverseWorkflowSpec,
            v2_specs.workflows.ReverseWorkflowSpec
        )

    def test_task_spec_imports(self):
        self.assertEqual(
            specs.DirectTaskDefaultsSpec,
            v2_specs.tasks.DirectTaskDefaultsSpec
        )

        self.assertEqual(
            specs.DirectTaskSpec,
            v2_specs.tasks.DirectTaskSpec
        )

        self.assertEqual(
            specs.ReverseTaskDefaultsSpec,
            v2_specs.tasks.ReverseTaskDefaultsSpec
        )

        self.assertEqual(
            specs.ReverseTaskSpec,
            v2_specs.tasks.ReverseTaskSpec
        )
