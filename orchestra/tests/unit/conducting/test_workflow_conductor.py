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

import copy

from orchestra import conducting
from orchestra import graphing
from orchestra.specs import native as specs
from orchestra import states
from orchestra.tests.unit import base
from orchestra.utils import dictionary as dx


class WorkflowConductorTest(base.WorkflowConductorTest):

    def _prep_conductor(self, inputs=None, state=None):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        input:
          - a
          - b: False

        output:
          data:
            a: <% $.a %>
            b: <% $.b %>
            c: <% $.c %>

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  c: 'xyz'
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
          task3:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task4
          task4:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task5
          task5:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)

        if inputs:
            conductor = conducting.WorkflowConductor(spec, **inputs)
        else:
            conductor = conducting.WorkflowConductor(spec)

        if state:
            self.assertIsNone(conductor._workflow_state)
            self.assertIsNone(conductor.get_workflow_state())
            conductor.set_workflow_state(state)
            self.assertEqual(conductor._workflow_state, state)
            self.assertEqual(conductor.get_workflow_state(), state)
        else:
            self.assertIsNone(conductor._workflow_state)
            self.assertIsNone(conductor.get_workflow_state())

        self.assertIsNone(conductor._graph)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)

        self.assertIsNone(conductor._flow)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

        user_inputs = inputs or {}
        default_inputs = {'b': False}
        init_ctx_value = dx.merge_dicts(default_inputs, user_inputs, True)

        expected_ctx_entry = {'srcs': [], 'value': init_ctx_value}
        self.assertDictEqual(conductor._inputs, user_inputs)
        self.assertDictEqual(conductor.inputs, user_inputs)
        self.assertDictEqual(conductor.get_workflow_initial_context(), expected_ctx_entry)

        return conductor

    def test_init(self):
        default_inputs = {'b': False}
        conductor = self._prep_conductor()

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'inputs': {},
            'state': None,
            'flow': {
                'staged': {'task1': {'ctxs': [0]}},
                'tasks': {},
                'sequence': [],
                'contexts': [{'srcs': [], 'value': default_inputs}]
            }
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertIsNone(conductor._workflow_state)
        self.assertIsNone(conductor.get_workflow_state())
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

    def test_init_with_inputs(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'inputs': inputs,
            'state': None,
            'flow': {
                'staged': {'task1': {'ctxs': [0]}},
                'tasks': {},
                'sequence': [],
                'contexts': [{'srcs': [], 'value': inputs}]
            }
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertIsNone(conductor._workflow_state)
        self.assertIsNone(conductor.get_workflow_state())
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

    def test_init_with_partial_inputs(self):
        inputs = {'a': 123}
        default_inputs = {'b': False}
        expected_initial_ctx = dx.merge_dicts(inputs, default_inputs, True)
        conductor = self._prep_conductor(inputs)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'inputs': inputs,
            'state': None,
            'flow': {
                'staged': {'task1': {'ctxs': [0]}},
                'tasks': {},
                'sequence': [],
                'contexts': [{'srcs': [], 'value': expected_initial_ctx}]
            }
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertIsNone(conductor._workflow_state)
        self.assertIsNone(conductor.get_workflow_state())
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

    def test_serialization(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)

        # Mock task flows.
        for i in range(1, 6):
            task_name = 'task' + str(i)
            conductor.update_task_flow(task_name, states.RUNNING)
            conductor.update_task_flow(task_name, states.SUCCEEDED)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'state': conductor.get_workflow_state(),
            'flow': conductor.flow.serialize(),
            'inputs': conductor.inputs
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)
        self.assertEqual(len(conductor.flow.tasks), 5)
        self.assertEqual(len(conductor.flow.sequence), 5)

    def test_get_workflow_initial_context(self):
        conductor = self._prep_conductor()

        expected_ctx_entry = {'srcs': [], 'value': {'b': False}}

        self.assertDictEqual(conductor.get_workflow_initial_context(), expected_ctx_entry)

    def test_get_workflow_initial_context_with_inputs(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)

        expected_ctx_entry = {'srcs': [], 'value': inputs}

        self.assertDictEqual(conductor.get_workflow_initial_context(), expected_ctx_entry)

    def test_get_start_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        next_task_name = 'task1'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_ctx_value = {'b': False}
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_start_tasks(), expected_tasks)

    def test_get_start_tasks_when_graph_paused(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.set_workflow_state(states.PAUSING)
        self.assertListEqual(conductor.get_start_tasks(), [])

        conductor.set_workflow_state(states.PAUSED)
        self.assertListEqual(conductor.get_start_tasks(), [])

    def test_get_start_tasks_when_graph_canceled(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.set_workflow_state(states.CANCELING)
        self.assertListEqual(conductor.get_start_tasks(), [])

        conductor.set_workflow_state(states.CANCELED)
        self.assertListEqual(conductor.get_start_tasks(), [])

    def test_get_start_tasks_when_graph_abended(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.set_workflow_state(states.FAILED)
        self.assertListEqual(conductor.get_start_tasks(), [])

    def test_get_task(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        task = conductor.get_task(task_name)
        self.assertEqual(task['id'], task_name)
        self.assertEqual(task['name'], task_name)
        self.assertDictEqual(task['ctx'], {'b': False})
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)

        task_name = 'task2'
        task = conductor.get_task(task_name)
        self.assertEqual(task['id'], task_name)
        self.assertEqual(task['name'], task_name)
        self.assertDictEqual(task['ctx'], {'b': False, 'c': 'xyz'})

    def test_get_next_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        for i in range(1, 5):
            task_name = 'task' + str(i)
            conductor.update_task_flow(task_name, states.RUNNING)
            conductor.update_task_flow(task_name, states.SUCCEEDED)

            next_task_name = 'task' + str(i + 1)
            next_task_spec = conductor.spec.tasks.get_task(next_task_name)
            expected_ctx_val = {'b': False, 'c': 'xyz'}
            expected_task = self.format_task_item(next_task_name, expected_ctx_val, next_task_spec)
            self.assert_task_list(conductor.get_next_tasks(task_name), [expected_task])

    def test_get_next_tasks_when_this_task_paused(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        ctx_value = {'b': False, 'c': 'xyz'}
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        expected_tasks = [self.format_task_item(next_task_name, ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        task_name = 'task2'
        next_task_name = 'task3'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.PAUSING)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
        conductor.update_task_flow(task_name, states.PAUSED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
        conductor.update_task_flow(task_name, states.RESUMING)
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        expected_tasks = [self.format_task_item(next_task_name, ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

    def test_get_next_tasks_when_graph_paused(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        ctx_value = {'b': False, 'c': 'xyz'}
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        expected_tasks = [self.format_task_item(next_task_name, ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        conductor.set_workflow_state(states.PAUSING)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        conductor.set_workflow_state(states.PAUSED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        conductor.set_workflow_state(states.RESUMING)
        expected_tasks = [self.format_task_item(next_task_name, ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

    def test_get_next_tasks_when_this_task_canceled(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        ctx_value = {'b': False, 'c': 'xyz'}
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        expected_tasks = [self.format_task_item(next_task_name, ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        task_name = 'task2'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.CANCELING)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
        conductor.update_task_flow(task_name, states.CANCELED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

    def test_get_next_tasks_when_graph_canceled(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        ctx_value = {'b': False, 'c': 'xyz'}
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        expected_tasks = [self.format_task_item(next_task_name, ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        conductor.set_workflow_state(states.CANCELING)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        conductor.set_workflow_state(states.CANCELED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

    def test_get_next_tasks_when_this_task_abended(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        ctx_value = {'b': False, 'c': 'xyz'}
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        expected_tasks = [self.format_task_item(next_task_name, ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        task_name = 'task2'
        conductor.graph.update_transition('task2', 'task3', 0, criteria=['<% succeeded() %>'])
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.FAILED)
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

    def test_get_next_tasks_when_graph_abended(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        ctx_value = {'b': False, 'c': 'xyz'}
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        expected_tasks = [self.format_task_item(next_task_name, ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)
        expected_tasks = [self.format_task_item(next_task_name, ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        conductor.set_workflow_state(states.FAILED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

    def test_get_task_initial_context(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)

        task_name = 'task1'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)

        task1_in_ctx = {'srcs': [], 'value': copy.deepcopy(inputs)}
        self.assertDictEqual(conductor.get_task_initial_context(task_name), task1_in_ctx)

        task2_in_ctx = {'srcs': [0], 'value': dx.merge_dicts(copy.deepcopy(inputs), {'c': 'xyz'})}
        expected_context_list = [task1_in_ctx, task2_in_ctx]
        self.assertListEqual(conductor.flow.contexts, expected_context_list)

    def test_get_task_transition_contexts(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)

        # Use task1 to get context for task2 that is staged by not yet running.
        conductor.update_task_flow('task1', states.RUNNING)
        conductor.update_task_flow('task1', states.SUCCEEDED)
        task2_in_ctx = {'srcs': [0], 'value': dx.merge_dicts(copy.deepcopy(inputs), {'c': 'xyz'})}
        expected_contexts = {'task2__0': task2_in_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1'), expected_contexts)

        # Use task1 to get context for task2 that is alstaged running.
        conductor.update_task_flow('task2', states.RUNNING)
        task2_in_ctx = {'srcs': [0], 'value': dx.merge_dicts(copy.deepcopy(inputs), {'c': 'xyz'})}
        expected_contexts = {'task2__0': task2_in_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1'), expected_contexts)

        # Use task2 to get context for task3 that is not staged yet.
        self.assertDictEqual(conductor.get_task_transition_contexts('task2'), {})

        # Use task3 that is not yet staged to get context.
        self.assertRaises(
            Exception,
            conductor.get_task_transition_contexts,
            'task3'
        )

    def test_get_workflow_terminal_context_when_workflow_incomplete(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)
        self.assertRaises(Exception, conductor.get_workflow_terminal_context)

    def test_get_workflow_terminal_context_when_workflow_completed(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)

        for i in range(1, 6):
            task_name = 'task' + str(i)
            conductor.update_task_flow(task_name, states.RUNNING)
            conductor.update_task_flow(task_name, states.SUCCEEDED)

        expected_ctx_value = {'a': 123, 'b': True, 'c': 'xyz'}
        expected_ctx_entry = {'src': [4], 'term': True, 'value': expected_ctx_value}
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_ctx_entry)

    def test_get_workflow_output_when_workflow_incomplete(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)

        for i in range(1, 5):
            task_name = 'task' + str(i)
            conductor.update_task_flow(task_name, states.RUNNING)
            conductor.update_task_flow(task_name, states.SUCCEEDED)

        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.assertIsNone(conductor.get_workflow_output())

    def test_get_workflow_output_when_workflow_failed(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)

        for i in range(1, 5):
            task_name = 'task' + str(i)
            conductor.update_task_flow(task_name, states.RUNNING)
            conductor.update_task_flow(task_name, states.SUCCEEDED)

        task_name = 'task5'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.FAILED)

        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        self.assertIsNone(conductor.get_workflow_output())

    def test_get_workflow_output_when_workflow_succeeded(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)

        for i in range(1, 6):
            task_name = 'task' + str(i)
            conductor.update_task_flow(task_name, states.RUNNING)
            conductor.update_task_flow(task_name, states.SUCCEEDED)

        expected_output = {'data': {'a': 123, 'b': True, 'c': 'xyz'}}
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_set_workflow_canceling_when_no_active_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        conductor.set_workflow_state(states.CANCELING)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_set_workflow_canceled_when_no_active_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        conductor.set_workflow_state(states.CANCELED)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_set_workflow_canceling_when_has_active_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.set_workflow_state(states.CANCELING)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELING)

    def test_set_workflow_canceled_when_has_active_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.set_workflow_state(states.CANCELED)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELING)

    def test_set_workflow_pausing_when_no_active_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        conductor.set_workflow_state(states.PAUSING)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

    def test_set_workflow_paused_when_no_active_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.update_task_flow(task_name, states.SUCCEEDED)
        conductor.set_workflow_state(states.PAUSED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

    def test_set_workflow_pausing_when_has_active_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.set_workflow_state(states.PAUSING)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSING)

    def test_set_workflow_paused_when_has_active_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        conductor.update_task_flow(task_name, states.RUNNING)
        conductor.set_workflow_state(states.PAUSED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSING)
