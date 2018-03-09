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

from orchestra.specs import native as specs
from orchestra.tests.unit.specs.native import base


class WorkflowSpecTest(base.OrchestraWorkflowSpecTest):

    def test_exception_empty_definition(self):
        self.assertRaises(ValueError, specs.WorkflowSpec, None, {})
        self.assertRaises(ValueError, specs.WorkflowSpec, None, '')
        self.assertRaises(ValueError, specs.WorkflowSpec, None, None)

    def test_basic_spec_instantiation(self):
        wf_name = 'sequential'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertIsNotNone(wf_spec)
        self.assertEqual(wf_spec.description, 'A basic sequential workflow.')
        self.assertIsInstance(wf_spec.tasks, specs.TaskMappingSpec)
        self.assertEqual(len(wf_spec.tasks), 3)

        self.assertListEqual(
            sorted(list(wf_spec.tasks.keys())),
            ['task1', 'task2', 'task3']
        )

        # Verify model for task1.
        task1 = wf_spec.tasks['task1']

        self.assertIsInstance(task1, specs.TaskSpec)
        self.assertEqual(task1.action, 'std.noop')

        task1_transition_seqs = getattr(task1, 'next')

        self.assertIsInstance(
            task1_transition_seqs,
            specs.TaskTransitionSequenceSpec
        )

        self.assertIsInstance(
            task1_transition_seqs[0],
            specs.TaskTransitionSpec
        )

        self.assertEqual(
            getattr(task1_transition_seqs[0], 'when'),
            '<% succeeded() %>'
        )

        self.assertListEqual(
            getattr(task1_transition_seqs[0], 'do'),
            ['task2']
        )

        # Verify model for task2.
        task2 = wf_spec.tasks['task2']

        self.assertIsInstance(task2, specs.TaskSpec)
        self.assertEqual(task2.action, 'std.noop')

        task2_transition_seqs = getattr(task2, 'next')

        self.assertIsInstance(
            task2_transition_seqs,
            specs.TaskTransitionSequenceSpec
        )

        self.assertIsInstance(
            task2_transition_seqs[0],
            specs.TaskTransitionSpec
        )

        self.assertEqual(
            getattr(task2_transition_seqs[0], 'when'),
            '<% succeeded() %>'
        )

        self.assertEqual(
            getattr(task2_transition_seqs[0], 'do'),
            'task3'
        )

        # Verify model for task3.
        task3 = wf_spec.tasks['task3']

        self.assertIsInstance(task3, specs.TaskSpec)
        self.assertEqual(task3.action, 'std.noop')
        self.assertIsNone(getattr(task3, 'next'))

    def test_basic_spec_serialization(self):
        wf_name = 'sequential'
        wf_spec_1 = self.get_wf_spec(wf_name)
        wf_spec_2 = specs.WorkflowSpec.deserialize(wf_spec_1.serialize())

        self.assertDictEqual(wf_spec_2.serialize(), wf_spec_1.serialize())

    def test_get_start_tasks(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertListEqual(
            wf_spec.tasks.get_start_tasks(),
            [('task1', None)]
        )

    def test_is_join_task(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertFalse(wf_spec.tasks.is_join_task('task4'))
        self.assertTrue(wf_spec.tasks.is_join_task('task7'))

    def test_is_split_task(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertTrue(wf_spec.tasks.is_split_task('task4'))
        self.assertFalse(wf_spec.tasks.is_split_task('task7'))

    def test_not_in_cycle(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertFalse(wf_spec.tasks.in_cycle('task4'))

    def test_has_cycles(self):
        wf_name = 'cycle'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertTrue(wf_spec.tasks.has_cycles())

    def test_in_cycle(self):
        wf_name = 'cycle'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertFalse(wf_spec.tasks.in_cycle('prep'))
        self.assertTrue(wf_spec.tasks.in_cycle('task1'))
        self.assertTrue(wf_spec.tasks.in_cycle('task2'))
        self.assertTrue(wf_spec.tasks.in_cycle('task3'))

    def test_in_cycle_of_multiple(self):
        wf_name = 'cycles'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertFalse(wf_spec.tasks.in_cycle('prep'))
        self.assertTrue(wf_spec.tasks.in_cycle('task1'))
        self.assertTrue(wf_spec.tasks.in_cycle('task2'))
        self.assertTrue(wf_spec.tasks.in_cycle('task3'))
        self.assertTrue(wf_spec.tasks.in_cycle('task4'))
        self.assertTrue(wf_spec.tasks.in_cycle('task5'))

    def test_with_items(self):
        wf_name = 'with-items'
        wf_spec = self.get_wf_spec(wf_name)
        t1 = wf_spec.tasks['task1']
        with_attr = getattr(t1, 'with')

        self.assertIsNotNone(with_attr)
        self.assertEqual(with_attr.items, 'member in <% $.members %>')
        self.assertEqual(with_attr.concurrency, '<% $.batch_size %>')

    def test_with_multi_items(self):
        wf_name = 'with-multi-items'
        wf_spec = self.get_wf_spec(wf_name)
        t1 = wf_spec.tasks['task1']
        with_attr = getattr(t1, 'with')

        self.assertIsNotNone(with_attr)
        self.assertIsInstance(with_attr.items, list)
        self.assertEqual(len(with_attr.items), 2)
        self.assertIn('member in <% $.members %>', with_attr.items)
        self.assertIn('message in <% $.messages %>', with_attr.items)
        self.assertEqual(with_attr.concurrency, '<% $.batch_size %>')
