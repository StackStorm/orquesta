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

from orquesta import conducting
from orquesta import events
from orquesta import exceptions as exc
from orquesta import graphing
from orquesta.specs import native as specs
from orquesta import states
from orquesta.tests.unit import base


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

    def _prep_conductor(self, context=None, inputs=None, state=None):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task2, task5
          task2:
            action: core.noop
            next:
              - do: task3
          task3:
            action: core.noop
            next:
              - do: task4
          task4:
            action: core.noop
            next:
              - do: task2
          task5:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)

        kwargs = {
            'context': context if context is not None else None,
            'inputs': inputs if inputs is not None else None
        }

        conductor = conducting.WorkflowConductor(spec, **kwargs)

        if state:
            conductor.request_workflow_state(state)

        return conductor

    def test_add_task_flow(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.add_task_flow('task1', in_ctx_idx=0)
        expected_task_flow_item = {'id': 'task1', 'ctx': 0}

        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

    def test_add_task_flow_no_context(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.add_task_flow('task1')
        expected_task_flow_item = {'id': 'task1', 'ctx': None}

        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

    def test_update_task_flow(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.RUNNING))
        expected_task_flow_item = {'id': 'task1', 'state': 'running', 'ctx': 0}

        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result='foobar')
        conductor.update_task_flow('task1', ac_ex_event)

        expected_task_flow_item = {
            'id': 'task1',
            'state': 'succeeded',
            'task2__0': True,
            'task5__0': True,
            'ctx': 0
        }

        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

    def test_update_task_flow_with_failed_event(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.RUNNING))
        expected_task_flow_item = {'id': 'task1', 'state': 'running', 'ctx': 0}

        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

        ac_ex_event = events.ActionExecutionEvent(states.FAILED, result={'stdout': 'boom!'})
        conductor.update_task_flow('task1', ac_ex_event)

        expected_task_flow_item = {
            'id': 'task1',
            'state': 'failed',
            'task2__0': False,
            'task5__0': False,
            'ctx': 0
        }

        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

        expected_errors = [
            {
                'type': 'error',
                'message': 'Execution failed. See result for details.',
                'task_id': 'task1',
                'result': {'stdout': 'boom!'}
            }
        ]

        self.assertListEqual(conductor.errors, expected_errors)

    def test_update_task_flow_for_not_ready_task(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        self.assertRaises(
            exc.InvalidTaskFlowEntry,
            conductor.update_task_flow,
            'task2',
            events.ActionExecutionEvent(states.RUNNING)
        )

    def test_update_task_flow_for_nonexistent_task(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        self.assertRaises(
            exc.InvalidTask,
            conductor.update_task_flow,
            'task999',
            events.ActionExecutionEvent(states.RUNNING)
        )

    def test_update_invalid_state_to_task_flow_item(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        self.assertRaises(
            exc.InvalidState,
            events.ActionExecutionEvent,
            'foobar'
        )

        self.assertRaises(
            TypeError,
            conductor.update_task_flow,
            'task1',
            'foobar'
        )

        # When transition is not valid, the task state is not changed. For the test case below,
        # the state change from requested to succeeded is not a valid transition.
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.REQUESTED))
        expected_task_flow_item = {'id': 'task1', 'state': 'requested', 'ctx': 0}
        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.SUCCEEDED))
        expected_task_flow_item = {'id': 'task1', 'state': 'requested', 'ctx': 0}
        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

    def test_add_sequence_to_task_flow(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        # Update progress of task1 to task flow.
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.RUNNING))
        expected_task_flow_item = {'id': 'task1', 'state': 'running', 'ctx': 0}

        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result='foobar')
        conductor.update_task_flow('task1', ac_ex_event)

        expected_task_flow_item = {
            'id': 'task1',
            'state': 'succeeded',
            'task2__0': True,
            'task5__0': True,
            'ctx': 0
        }

        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertDictEqual(conductor.get_task_flow_entry('task1'), expected_task_flow_item)

        expected_sequence = [expected_task_flow_item]
        self.assertListEqual(conductor.flow.sequence, expected_sequence)

        # Update progress of task2 to task flow.
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.RUNNING))
        expected_task_flow_item = {'id': 'task2', 'state': 'running', 'ctx': 0}

        self.assertEqual(conductor._get_task_flow_idx('task2'), 1)
        self.assertDictEqual(conductor.get_task_flow_entry('task2'), expected_task_flow_item)

        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result='foobar')
        conductor.update_task_flow('task2', ac_ex_event)
        expected_task_flow_item = {'id': 'task2', 'state': 'succeeded', 'task3__0': True, 'ctx': 0}

        self.assertEqual(conductor._get_task_flow_idx('task2'), 1)
        self.assertDictEqual(conductor.get_task_flow_entry('task2'), expected_task_flow_item)

        expected_sequence = [
            {'id': 'task1', 'state': 'succeeded', 'task2__0': True, 'task5__0': True, 'ctx': 0},
            {'id': 'task2', 'state': 'succeeded', 'task3__0': True, 'ctx': 0}
        ]

        self.assertListEqual(conductor.flow.sequence, expected_sequence)

    def test_add_cycle_to_task_flow(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        # Check that there's a cycle in the graph.
        self.assertFalse(conductor.graph.in_cycle('task1'))
        self.assertTrue(conductor.graph.in_cycle('task2'))
        self.assertTrue(conductor.graph.in_cycle('task3'))
        self.assertTrue(conductor.graph.in_cycle('task4'))

        # Add a cycle to the task flow.
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.SUCCEEDED))
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.SUCCEEDED))
        conductor.update_task_flow('task3', events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow('task3', events.ActionExecutionEvent(states.SUCCEEDED))
        conductor.update_task_flow('task4', events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow('task4', events.ActionExecutionEvent(states.SUCCEEDED))
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.RUNNING))

        # Check the reference pointer. Task2 points to the latest instance.
        self.assertEqual(conductor._get_task_flow_idx('task1'), 0)
        self.assertEqual(conductor._get_task_flow_idx('task2'), 4)
        self.assertEqual(conductor._get_task_flow_idx('task3'), 2)
        self.assertEqual(conductor._get_task_flow_idx('task4'), 3)

        # Check sequence.
        expected_sequence = [
            {'id': 'task1', 'state': 'succeeded', 'task2__0': True, 'task5__0': True, 'ctx': 0},
            {'id': 'task2', 'state': 'succeeded', 'task3__0': True, 'ctx': 0},
            {'id': 'task3', 'state': 'succeeded', 'task4__0': True, 'ctx': 0},
            {'id': 'task4', 'state': 'succeeded', 'task2__0': True, 'ctx': 0},
            {'id': 'task2', 'state': 'running', 'ctx': 0},
        ]

        self.assertListEqual(conductor.flow.sequence, expected_sequence)
