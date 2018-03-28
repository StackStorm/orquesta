# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from orchestra import conducting
from orchestra import graphing
from orchestra import exceptions as exc
from orchestra.specs import mock as specs
from orchestra import states
from orchestra.tests.unit import base


class WorkflowConductorTaskFlowTest(base.WorkflowConductorTest):

    def _add_tasks(self, wf_graph):
        for i in range(1, 6):
            task_name = 'task' + str(i)
            wf_graph.add_task(task_name, name=task_name)

    def _add_transitions(self, wf_graph):
        wf_graph.add_transition('task1', 'task2')
        wf_graph.add_transition('task1', 'task5')
        wf_graph.add_transition('task2', 'task3')
        wf_graph.add_transition('task3', 'task4')
        wf_graph.add_transition('task4', 'task2')

    def _prep_graph(self):
        wf_graph = graphing.WorkflowGraph()

        self._add_tasks(wf_graph)
        self._add_transitions(wf_graph)

        return wf_graph

    def _prep_conductor(self):
        wf_def = {'name': 'foobar'}
        wf_spec = specs.WorkflowSpec(wf_def)
        wf_graph = self._prep_graph()

        return conducting.WorkflowConductor(wf_spec, graph=wf_graph)

    def test_add_task_flow_entry(self):
        conductor = self._prep_conductor()

        conductor.add_task_flow_entry('task1')
        expected_task_flow_item = {'id': 'task1'}

        self.assertEqual(conductor.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

    def test_update_task_flow_entry(self):
        conductor = self._prep_conductor()

        conductor.update_task_flow_entry('task1', states.RUNNING)
        expected_task_flow_item = {'id': 'task1', 'state': 'running'}

        self.assertEqual(conductor.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

        conductor.update_task_flow_entry('task1', states.SUCCEEDED, context={'var1': 'foobar'})

        expected_task_flow_item = {
            'id': 'task1',
            'state': 'succeeded',
            'task2__0': True,
            'task5__0': True
        }

        self.assertEqual(conductor.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

    def test_update_task_flow_entry_for_nonexistent_task(self):
        conductor = self._prep_conductor()

        self.assertRaises(
            exc.InvalidTask,
            conductor.update_task_flow_entry,
            'task999',
            states.RUNNING
        )

    def test_update_invalid_state_to_task_flow_item(self):
        conductor = self._prep_conductor()

        self.assertRaises(
            exc.InvalidState,
            conductor.update_task_flow_entry,
            'task1',
            'foobar'
        )

        self.assertRaises(
            exc.InvalidStateTransition,
            conductor.update_task_flow_entry,
            'task1',
            states.SUCCEEDED
        )

    def test_add_sequence_to_task_flow(self):
        conductor = self._prep_conductor()

        # Update progress of task1 to task flow.
        conductor.update_task_flow_entry('task1', states.RUNNING)
        expected_task_flow_item = {'id': 'task1', 'state': 'running'}

        self.assertEqual(conductor.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

        conductor.update_task_flow_entry('task1', states.SUCCEEDED, context={'var1': 'foobar'})

        expected_task_flow_item = {
            'id': 'task1',
            'state': 'succeeded',
            'task2__0': True,
            'task5__0': True
        }

        self.assertEqual(conductor.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

        expected_sequence = [
            {'id': 'task1', 'state': 'succeeded', 'task2__0': True, 'task5__0': True}
        ]

        self.assertListEqual(conductor.flow.sequence, expected_sequence)

        # Update progress of task2 to task flow.
        conductor.update_task_flow_entry('task2', states.RUNNING)
        expected_task_flow_item = {'id': 'task2', 'state': 'running'}

        self.assertEqual(conductor.get_task_flow_idx('task2'), 1)
        self.assertDictEqual(conductor.get_task_flow_entry('task2'), expected_task_flow_item)

        conductor.update_task_flow_entry('task2', states.SUCCEEDED, context={'var1': 'foobar'})
        expected_task_flow_item = {'id': 'task2', 'state': 'succeeded', 'task3__0': True}

        self.assertEqual(conductor.get_task_flow_idx('task2'), 1)
        self.assertDictEqual(conductor.get_task_flow_entry('task2'), expected_task_flow_item)

        expected_sequence = [
            {'id': 'task1', 'state': 'succeeded', 'task2__0': True, 'task5__0': True},
            {'id': 'task2', 'state': 'succeeded', 'task3__0': True}
        ]

        self.assertListEqual(conductor.flow.sequence, expected_sequence)

    def test_add_cycle_to_task_flow(self):
        conductor = self._prep_conductor()

        # Check that there's a cycle in the graph.
        self.assertFalse(conductor.graph.in_cycle('task1'))
        self.assertTrue(conductor.graph.in_cycle('task2'))
        self.assertTrue(conductor.graph.in_cycle('task3'))
        self.assertTrue(conductor.graph.in_cycle('task4'))

        # Add a cycle to the task flow.
        conductor.update_task_flow_entry('task1', states.RUNNING)
        conductor.update_task_flow_entry('task1', states.SUCCEEDED)
        conductor.update_task_flow_entry('task2', states.RUNNING)
        conductor.update_task_flow_entry('task2', states.SUCCEEDED)
        conductor.update_task_flow_entry('task3', states.RUNNING)
        conductor.update_task_flow_entry('task3', states.SUCCEEDED)
        conductor.update_task_flow_entry('task4', states.RUNNING)
        conductor.update_task_flow_entry('task4', states.SUCCEEDED)
        conductor.update_task_flow_entry('task2', states.RUNNING)

        # Check the reference pointer. Task2 points to the latest instance.
        self.assertEqual(conductor.get_task_flow_idx('task1'), 0)
        self.assertEqual(conductor.get_task_flow_idx('task2'), 4)
        self.assertEqual(conductor.get_task_flow_idx('task3'), 2)
        self.assertEqual(conductor.get_task_flow_idx('task4'), 3)

        # Check sequence.
        expected_sequence = [
            {'id': 'task1', 'state': 'succeeded', 'task2__0': True, 'task5__0': True},
            {'id': 'task2', 'state': 'succeeded', 'task3__0': True},
            {'id': 'task3', 'state': 'succeeded', 'task4__0': True},
            {'id': 'task4', 'state': 'succeeded', 'task2__0': True},
            {'id': 'task2', 'state': 'running'},
        ]

        self.assertListEqual(conductor.flow.sequence, expected_sequence)
