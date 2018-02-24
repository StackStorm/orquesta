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

from orchestra import graphing
from orchestra import exceptions as exc
from orchestra import states
from orchestra.tests.unit import base


class WorkflowGraphTaskFlowTest(base.WorkflowGraphTest):

    def _add_tasks(self, wf_graph):
        for i in range(1, 6):
            wf_graph.add_task('task' + str(i))

    def _add_transitions(self, wf_graph):
        wf_graph.add_transition('task1', 'task2')
        wf_graph.add_transition('task1', 'task5')
        wf_graph.add_transition('task2', 'task3')
        wf_graph.add_transition('task3', 'task4')
        wf_graph.add_transition('task4', 'task2')

    def _prep_graph(self, wf_graph):
        self._add_tasks(wf_graph)
        self._add_transitions(wf_graph)

    def test_get_set_graph_state(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        self.assertIsNone(wf_graph.state)

        wf_graph.state = states.RUNNING
        self.assertEqual(wf_graph.state, states.RUNNING)

    def test_get_set_bad_graph_state(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        self.assertRaises(exc.InvalidState, wf_graph.state, 'foobar')
        self.assertIsNone(wf_graph.state)

    def test_add_task_flow_item(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        wf_graph.add_task_flow_item('task1')
        expected_task_flow_item = {'id': 'task1'}

        self.assertEqual(wf_graph.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(wf_graph.get_task_flow_item('task1'), expected_task_flow_item)

    def test_update_task_flow_item(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        wf_graph.update_task_flow_item('task1', states.RUNNING)
        expected_task_flow_item = {'id': 'task1', 'state': 'running'}

        self.assertEqual(wf_graph.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(wf_graph.get_task_flow_item('task1'), expected_task_flow_item)

        wf_graph.update_task_flow_item('task1', states.SUCCEEDED, context={'var1': 'foobar'})

        expected_task_flow_item = {
            'id': 'task1',
            'state': 'succeeded',
            'task2__0': True,
            'task5__0': True
        }

        self.assertEqual(wf_graph.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(wf_graph.get_task_flow_item('task1'), expected_task_flow_item)

    def test_update_task_flow_item_for_nonexistent_task(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        self.assertRaises(
            exc.InvalidTask,
            wf_graph.update_task_flow_item,
            'task999',
            states.RUNNING
        )

    def test_update_invalid_state_to_task_flow_item(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        self.assertRaises(
            exc.InvalidState,
            wf_graph.update_task_flow_item,
            'task1',
            'foobar'
        )

        self.assertRaises(
            exc.InvalidStateTransition,
            wf_graph.update_task_flow_item,
            'task1',
            states.SUCCEEDED
        )

    def test_add_sequence_to_task_flow(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        # Update progress of task1 to task flow.
        wf_graph.update_task_flow_item('task1', states.RUNNING)
        expected_task_flow_item = {'id': 'task1', 'state': 'running'}

        self.assertEqual(wf_graph.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(wf_graph.get_task_flow_item('task1'), expected_task_flow_item)

        wf_graph.update_task_flow_item('task1', states.SUCCEEDED, context={'var1': 'foobar'})

        expected_task_flow_item = {
            'id': 'task1',
            'state': 'succeeded',
            'task2__0': True,
            'task5__0': True
        }

        self.assertEqual(wf_graph.get_task_flow_idx('task1'), 0)
        self.assertDictEqual(wf_graph.get_task_flow_item('task1'), expected_task_flow_item)

        expected_sequence = [
            {'id': 'task1', 'state': 'succeeded', 'task2__0': True, 'task5__0': True}
        ]

        self.assertListEqual(wf_graph.sequence, expected_sequence)

        # Update progress of task2 to task flow.
        wf_graph.update_task_flow_item('task2', states.RUNNING)
        expected_task_flow_item = {'id': 'task2', 'state': 'running'}

        self.assertEqual(wf_graph.get_task_flow_idx('task2'), 1)
        self.assertDictEqual(wf_graph.get_task_flow_item('task2'), expected_task_flow_item)

        wf_graph.update_task_flow_item('task2', states.SUCCEEDED, context={'var1': 'foobar'})
        expected_task_flow_item = {'id': 'task2', 'state': 'succeeded', 'task3__0': True}

        self.assertEqual(wf_graph.get_task_flow_idx('task2'), 1)
        self.assertDictEqual(wf_graph.get_task_flow_item('task2'), expected_task_flow_item)

        expected_sequence = [
            {'id': 'task1', 'state': 'succeeded', 'task2__0': True, 'task5__0': True},
            {'id': 'task2', 'state': 'succeeded', 'task3__0': True}
        ]

        self.assertListEqual(wf_graph.sequence, expected_sequence)

    def test_add_cycle_to_task_flow(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        # Check that there's a cycle in the graph.
        self.assertFalse(wf_graph.in_cycle('task1'))
        self.assertTrue(wf_graph.in_cycle('task2'))
        self.assertTrue(wf_graph.in_cycle('task3'))
        self.assertTrue(wf_graph.in_cycle('task4'))

        # Add a cycle to the task flow.
        wf_graph.update_task_flow_item('task1', states.RUNNING)
        wf_graph.update_task_flow_item('task1', states.SUCCEEDED)
        wf_graph.update_task_flow_item('task2', states.RUNNING)
        wf_graph.update_task_flow_item('task2', states.SUCCEEDED)
        wf_graph.update_task_flow_item('task3', states.RUNNING)
        wf_graph.update_task_flow_item('task3', states.SUCCEEDED)
        wf_graph.update_task_flow_item('task4', states.RUNNING)
        wf_graph.update_task_flow_item('task4', states.SUCCEEDED)
        wf_graph.update_task_flow_item('task2', states.RUNNING)

        # Check the reference pointer. Task2 points to the latest instance.
        self.assertEqual(wf_graph.get_task_flow_idx('task1'), 0)
        self.assertEqual(wf_graph.get_task_flow_idx('task2'), 4)
        self.assertEqual(wf_graph.get_task_flow_idx('task3'), 2)
        self.assertEqual(wf_graph.get_task_flow_idx('task4'), 3)

        # Check sequence.
        expected_sequence = [
            {'id': 'task1', 'state': 'succeeded', 'task2__0': True, 'task5__0': True},
            {'id': 'task2', 'state': 'succeeded', 'task3__0': True},
            {'id': 'task3', 'state': 'succeeded', 'task4__0': True},
            {'id': 'task4', 'state': 'succeeded', 'task2__0': True},
            {'id': 'task2', 'state': 'running'},
        ]

        self.assertListEqual(wf_graph.sequence, expected_sequence)
