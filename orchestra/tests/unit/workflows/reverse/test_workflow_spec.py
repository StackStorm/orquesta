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

from orchestra.specs import v2 as specs
from orchestra.tests.unit import base


class ReverseWorkflowSpecTest(base.WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'reverse'
        super(ReverseWorkflowSpecTest, cls).setUpClass()

    def test_exception_empty_definition(self):
        self.assertRaises(ValueError, specs.ReverseWorkflowSpec, {})
        self.assertRaises(ValueError, specs.ReverseWorkflowSpec, '')
        self.assertRaises(ValueError, specs.ReverseWorkflowSpec, None)

    def test_get_next_tasks(self):
        wf_name = 'sequential'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_next_tasks('task1'),
            [('task2', None, None)]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task2'),
            [('task3', None, None)]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task3'),
            []
        )

    def test_get_prev_tasks(self):
        wf_name = 'sequential'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_prev_tasks('task1'),
            []
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task2'),
            [('task1', None, None)]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task3'),
            [('task2', None, None)]
        )

    def test_get_start_tasks(self):
        wf_name = 'sequential'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_start_tasks(),
            ['task1']
        )

    def test_not_in_cycle(self):
        wf_name = 'sequential'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertFalse(wf_spec.in_cycle('task1'))

    def test_has_cycles(self):
        wf_name = 'cycle'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertTrue(wf_spec.has_cycles())

    def test_in_cycle(self):
        wf_name = 'cycle'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertTrue(wf_spec.in_cycle('task1'))
