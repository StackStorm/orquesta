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


class DirectWorkflowSpecTest(base.WorkflowSpecTest):
    fixture_rel_path = 'direct'

    def test_exception_empty_definition(self):
        self.assertRaises(ValueError, specs.DirectWorkflowSpec, {})
        self.assertRaises(ValueError, specs.DirectWorkflowSpec, '')
        self.assertRaises(ValueError, specs.DirectWorkflowSpec, None)

    def test_get_next_tasks(self):
        wf_name = 'split'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.DirectWorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_next_tasks('task1'),
            [('task2', None, 'on-success'), ('task3', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task2'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task3'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task4'),
            [('task5', None, 'on-success'), ('task6', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task5'),
            [('task7', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task6'),
            [('task7', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task7'),
            []
        )

    def test_get_prev_tasks(self):
        wf_name = 'split'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.DirectWorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_prev_tasks('task1'),
            []
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task2'),
            [('task1', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task3'),
            [('task1', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task4'),
            [('task2', None, 'on-success'), ('task3', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task5'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task6'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task7'),
            [('task5', None, 'on-success'), ('task6', None, 'on-success')]
        )

    def test_get_start_tasks(self):
        wf_name = 'split'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.DirectWorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_start_tasks(),
            ['task1']
        )

    def test_is_join_task(self):
        wf_name = 'split'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.DirectWorkflowSpec(wf_def)

        self.assertFalse(wf_spec.is_join_task('task4'))
        self.assertTrue(wf_spec.is_join_task('task7'))

    def test_is_split_task(self):
        wf_name = 'split'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.DirectWorkflowSpec(wf_def)

        self.assertTrue(wf_spec.is_split_task('task4'))
        self.assertFalse(wf_spec.is_split_task('task7'))

    def test_not_in_cycle(self):
        wf_name = 'split'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.DirectWorkflowSpec(wf_def)

        self.assertFalse(wf_spec.in_cycle('task4'))

    def test_has_cycles(self):
        wf_name = 'cycle'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.DirectWorkflowSpec(wf_def)

        self.assertTrue(wf_spec.has_cycles())

    def test_in_cycle(self):
        wf_name = 'cycle'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.DirectWorkflowSpec(wf_def)

        self.assertTrue(wf_spec.in_cycle('task1'))


class ReverseWorkflowSpecTest(base.WorkflowSpecTest):
    fixture_rel_path = 'reverse'

    def test_exception_empty_definition(self):
        self.assertRaises(ValueError, specs.ReverseWorkflowSpec, {})
        self.assertRaises(ValueError, specs.ReverseWorkflowSpec, '')
        self.assertRaises(ValueError, specs.ReverseWorkflowSpec, None)

    def test_get_next_tasks(self):
        wf_name = 'sequential'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
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
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
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
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_start_tasks(),
            ['task1']
        )

    def test_not_in_cycle(self):
        wf_name = 'sequential'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertFalse(wf_spec.in_cycle('task1'))

    def test_has_cycles(self):
        wf_name = 'cycle'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertTrue(wf_spec.has_cycles())

    def test_in_cycle(self):
        wf_name = 'cycle'
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = specs.ReverseWorkflowSpec(wf_def)

        self.assertTrue(wf_spec.in_cycle('task1'))
