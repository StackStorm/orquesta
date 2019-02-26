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

import copy

from orquesta import conducting
from orquesta import exceptions as exc
from orquesta import graphing
from orquesta.specs import native as specs
from orquesta import states
from orquesta.tests.unit import base
from orquesta.utils import dictionary as dx


class WorkflowConductorTest(base.WorkflowConductorTest):

    def _prep_conductor(self, context=None, inputs=None, state=None):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        input:
          - a
          - b: False

        output:
          - data:
              a: <% ctx().a %>
              b: <% ctx().b %>
              c: <% ctx().c %>

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - c: 'xyz'
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

        kwargs = {
            'context': context if context is not None else None,
            'inputs': inputs if inputs is not None else None
        }

        conductor = conducting.WorkflowConductor(spec, **kwargs)

        self.assertIsNone(conductor._graph)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)

        self.assertIsNone(conductor._flow)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

        if state:
            self.assertEqual(conductor._workflow_state, states.UNSET)
            self.assertEqual(conductor.get_workflow_state(), states.UNSET)
            conductor.request_workflow_state(state)
            self.assertEqual(conductor._workflow_state, state)
            self.assertEqual(conductor.get_workflow_state(), state)
        else:
            self.assertEqual(conductor._workflow_state, states.UNSET)
            self.assertEqual(conductor.get_workflow_state(), states.UNSET)

        user_inputs = inputs or {}
        parent_context = context or {}

        self.assertDictEqual(conductor._inputs, user_inputs)
        self.assertDictEqual(conductor.get_workflow_input(), user_inputs)
        self.assertDictEqual(conductor._parent_ctx, parent_context)
        self.assertDictEqual(conductor.get_workflow_parent_context(), parent_context)

        default_inputs = {'a': None, 'b': False}
        init_ctx_value = dx.merge_dicts(default_inputs, user_inputs, True)
        init_ctx_value = dx.merge_dicts(init_ctx_value, parent_context, True)
        expected_ctx_entry = {'srcs': [], 'value': init_ctx_value}
        self.assertDictEqual(conductor.get_workflow_initial_context(), expected_ctx_entry)

        return conductor

    def test_init(self):
        conductor = self._prep_conductor()

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'context': {},
            'input': {},
            'output': None,
            'state': states.UNSET,
            'errors': [],
            'log': [],
            'flow': {
                'routes': [
                    []
                ],
                'staged': [
                    {'id': 'task1', 'route': 0, 'prev': {}, 'ctxs': [0], 'ready': True}
                ],
                'tasks': {},
                'sequence': [],
                'contexts': [
                    {'srcs': [], 'value': {'a': None, 'b': False}}
                ]
            }
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertEqual(conductor._workflow_state, states.UNSET)
        self.assertEqual(conductor.get_workflow_state(), states.UNSET)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

    def test_init_with_inputs(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs=inputs)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'context': {},
            'input': inputs,
            'output': None,
            'state': states.UNSET,
            'errors': [],
            'log': [],
            'flow': {
                'routes': [
                    []
                ],
                'staged': [
                    {'id': 'task1', 'route': 0, 'prev': {}, 'ctxs': [0], 'ready': True}
                ],
                'tasks': {},
                'sequence': [],
                'contexts': [
                    {'srcs': [], 'value': inputs}
                ]
            }
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertEqual(conductor._workflow_state, states.UNSET)
        self.assertEqual(conductor.get_workflow_state(), states.UNSET)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

    def test_init_with_partial_inputs(self):
        inputs = {'a': 123}
        default_inputs = {'b': False}
        expected_initial_ctx = dx.merge_dicts(inputs, default_inputs, True)
        conductor = self._prep_conductor(inputs=inputs)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'context': {},
            'input': inputs,
            'output': None,
            'state': states.UNSET,
            'errors': [],
            'log': [],
            'flow': {
                'routes': [
                    []
                ],
                'staged': [
                    {'id': 'task1', 'route': 0, 'prev': {}, 'ctxs': [0], 'ready': True}
                ],
                'tasks': {},
                'sequence': [],
                'contexts': [
                    {'srcs': [], 'value': expected_initial_ctx}
                ]
            }
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertEqual(conductor._workflow_state, states.UNSET)
        self.assertEqual(conductor.get_workflow_state(), states.UNSET)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

    def test_init_with_context(self):
        context = {'parent': {'ex_id': '12345'}}
        inputs = {'a': 123, 'b': True}
        init_ctx = dx.merge_dicts(copy.deepcopy(inputs), copy.deepcopy(context))

        conductor = self._prep_conductor(context=context, inputs=inputs)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'context': context,
            'input': inputs,
            'output': None,
            'state': states.UNSET,
            'errors': [],
            'log': [],
            'flow': {
                'routes': [
                    []
                ],
                'staged': [
                    {'id': 'task1', 'route': 0, 'prev': {}, 'ctxs': [0], 'ready': True}
                ],
                'tasks': {},
                'sequence': [],
                'contexts': [
                    {'srcs': [], 'value': init_ctx}
                ]
            }
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertEqual(conductor._workflow_state, states.UNSET)
        self.assertEqual(conductor.get_workflow_state(), states.UNSET)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

    def test_serialization(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        # Mock task flows.
        for i in range(1, 6):
            self.forward_task_states(conductor, 'task' + str(i), [states.RUNNING, states.SUCCEEDED])

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'state': conductor.get_workflow_state(),
            'flow': conductor.flow.serialize(),
            'context': conductor.get_workflow_parent_context(),
            'input': conductor.get_workflow_input(),
            'output': conductor.get_workflow_output(),
            'errors': conductor.errors,
            'log': conductor.log
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
        expected_ctx_entry = {'srcs': [], 'value': {'a': None, 'b': False}}
        self.assertDictEqual(conductor.get_workflow_initial_context(), expected_ctx_entry)

    def test_get_workflow_initial_context_with_inputs(self):
        inputs = {'a': 123, 'b': True}
        expected_ctx_entry = {'srcs': [], 'value': inputs}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)
        self.assertDictEqual(conductor.get_workflow_initial_context(), expected_ctx_entry)

    def test_get_start_tasks(self):
        inputs = {'a': 123}
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)
        self.assert_next_task(conductor, 'task1', expected_task_ctx)

    def test_get_start_tasks_when_graph_paused(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        conductor.request_workflow_state(states.PAUSING)
        self.assertListEqual(conductor.get_next_tasks(), [])

        conductor.request_workflow_state(states.PAUSED)
        self.assertListEqual(conductor.get_next_tasks(), [])

    def test_get_start_tasks_when_graph_canceled(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        conductor.request_workflow_state(states.CANCELING)
        self.assertListEqual(conductor.get_next_tasks(), [])

        conductor.request_workflow_state(states.CANCELED)
        self.assertListEqual(conductor.get_next_tasks(), [])

    def test_get_start_tasks_when_graph_abended(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        conductor.request_workflow_state(states.FAILED)
        self.assertListEqual(conductor.get_next_tasks(), [])

    def test_get_task(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_route = 0
        task_name = 'task1'
        expected_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        expected_ctx['__current_task'] = {'id': task_name, 'route': task_route}
        expected_ctx['__flow'] = conductor.flow.serialize()
        task = conductor.get_task(task_name, task_route)
        self.assertEqual(task['id'], task_name)
        self.assertEqual(task['route'], task_route)
        self.assertDictEqual(task['ctx'], expected_ctx)

        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        task_name = 'task2'
        expected_ctx = dx.merge_dicts(copy.deepcopy(expected_ctx), {'c': 'xyz'})
        expected_ctx['__current_task'] = {'id': task_name, 'route': task_route}
        expected_ctx['__flow'] = conductor.flow.serialize()
        task = conductor.get_task(task_name, task_route)
        self.assertEqual(task['id'], task_name)
        self.assertEqual(task['route'], task_route)
        self.assertDictEqual(task['ctx'], expected_ctx)

    def test_get_next_tasks(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        for i in range(1, 5):
            task_name = 'task' + str(i)
            next_task_name = 'task' + str(i + 1)
            expected_task_ctx = {'a': 123, 'b': False, 'c': 'xyz'}
            self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
            self.assert_next_task(conductor, next_task_name, expected_task_ctx)

    def test_get_next_tasks_repeatedly(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        expected_init_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})

        self.assert_next_task(conductor, task_name, expected_init_ctx)

        self.forward_task_states(conductor, task_name, [states.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assert_next_task(conductor, has_next_task=False)

        self.forward_task_states(conductor, task_name, [states.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        self.forward_task_states(conductor, next_task_name, [states.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assert_next_task(conductor, has_next_task=False)

        self.forward_task_states(conductor, task_name, [states.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)
        self.assert_next_task(conductor, has_next_task=False)

    def test_get_next_tasks_when_this_task_paused(self):
        inputs = {'a': 123}
        expected_init_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        task_name = 'task2'
        next_task_name = 'task3'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.PAUSING])
        self.assert_next_task(conductor, has_next_task=False)
        self.forward_task_states(conductor, task_name, [states.PAUSED])
        self.assert_next_task(conductor, has_next_task=False)

        # After the previous task is paused, since there is no other tasks running,
        # the workflow is paused. The workflow needs to be resumed manually.
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)
        conductor.request_workflow_state(states.RESUMING)
        self.assertEqual(conductor.get_workflow_state(), states.RESUMING)

        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

    def test_get_next_tasks_when_graph_paused(self):
        inputs = {'a': 123}
        expected_init_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        conductor.request_workflow_state(states.PAUSING)
        self.assert_next_task(conductor, has_next_task=False)

        conductor.request_workflow_state(states.PAUSED)
        self.assert_next_task(conductor, has_next_task=False)

        conductor.request_workflow_state(states.RESUMING)
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

    def test_get_next_tasks_when_this_task_canceled(self):
        inputs = {'a': 123}
        expected_init_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        task_name = 'task2'
        next_task_name = 'task3'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.CANCELING])
        self.assert_next_task(conductor, has_next_task=False)
        self.forward_task_states(conductor, task_name, [states.CANCELED])
        self.assert_next_task(conductor, has_next_task=False)

        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_get_next_tasks_when_graph_canceled(self):
        inputs = {'a': 123}
        expected_init_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        conductor.request_workflow_state(states.CANCELING)
        self.assert_next_task(conductor, has_next_task=False)

        conductor.request_workflow_state(states.CANCELED)
        self.assert_next_task(conductor, has_next_task=False)

    def test_get_next_tasks_when_this_task_abended(self):
        inputs = {'a': 123}
        expected_init_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        task_name = 'task2'
        next_task_name = 'task3'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.FAILED])
        self.assert_next_task(conductor, has_next_task=False)

        self.assertEqual(conductor.get_workflow_state(), states.FAILED)

    def test_get_next_tasks_when_graph_abended(self):
        inputs = {'a': 123}
        expected_init_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        conductor.request_workflow_state(states.FAILED)
        self.assert_next_task(conductor, has_next_task=False)

    def test_get_task_initial_context(self):
        inputs = {'a': 123}
        expected_init_ctx = dx.merge_dicts(copy.deepcopy(inputs), {'b': False})
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_route = 0
        task_name = 'task1'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        expected_ctx = {'srcs': [], 'value': expected_init_ctx}
        task1_in_ctx = conductor.get_task_initial_context(task_name, task_route)
        self.assertDictEqual(task1_in_ctx, expected_ctx)

        task2_in_ctx = {'srcs': [0], 'value': expected_task_ctx}
        expected_context_list = [task1_in_ctx, task2_in_ctx]
        self.assertListEqual(conductor.flow.contexts, expected_context_list)

    def test_get_task_transition_contexts(self):
        inputs = {'a': 123, 'b': True}
        expected_init_ctx = copy.deepcopy(inputs)
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        # Get context for task2 that is staged by not yet running.
        task_route = 0
        task_name = 'task1'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        next_task_in_ctx = {'srcs': [0], 'value': expected_task_ctx}
        expected_task_transition_ctx = {'%s__t0' % next_task_name: next_task_in_ctx}

        self.assertDictEqual(
            conductor.get_task_transition_contexts(task_name, task_route),
            expected_task_transition_ctx
        )

        # Get context for task2 that is already running.
        task_name = 'task2'
        next_task_name = 'task3'
        self.forward_task_states(conductor, task_name, [states.RUNNING])

        expected_task_transition_ctx = {}

        self.assertDictEqual(
            conductor.get_task_transition_contexts(task_name, task_route),
            expected_task_transition_ctx
        )

        # Get context for task3 that is not staged yet.
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name, task_route), {})

        # Get transition context for task3 that has not yet run.
        self.assertRaises(
            exc.InvalidTaskFlowEntry,
            conductor.get_task_transition_contexts,
            next_task_name,
            task_route
        )

    def test_get_workflow_terminal_context_when_workflow_incomplete(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)
        self.assertRaises(exc.WorkflowContextError, conductor.get_workflow_terminal_context)

    def test_get_workflow_terminal_context_when_workflow_completed(self):
        inputs = {'a': 123, 'b': True}
        expected_init_ctx = copy.deepcopy(inputs)
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        for i in range(1, 6):
            task_name = 'task' + str(i)
            self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        expected_term_ctx_entry = {'src': [4], 'term': True, 'value': expected_task_ctx}
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx_entry)

    def test_get_workflow_output_when_workflow_incomplete(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        for i in range(1, 5):
            task_name = 'task' + str(i)
            self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.assertIsNone(conductor.get_workflow_output())

    def test_get_workflow_output_when_workflow_failed(self):
        inputs = {'a': 123, 'b': True}
        expected_init_ctx = copy.deepcopy(inputs)
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        for i in range(1, 5):
            task_name = 'task' + str(i)
            self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        task_name = 'task5'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.FAILED])

        expected_output = {'data': expected_task_ctx}
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_get_workflow_output_when_workflow_succeeded(self):
        inputs = {'a': 123, 'b': True}
        expected_init_ctx = copy.deepcopy(inputs)
        expected_task_ctx = dx.merge_dicts(copy.deepcopy(expected_init_ctx), {'c': 'xyz'})
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        for i in range(1, 6):
            task_name = 'task' + str(i)
            self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        expected_output = {'data': expected_task_ctx}
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_set_workflow_canceling_when_no_active_tasks(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        conductor.request_workflow_state(states.CANCELING)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_set_workflow_canceled_when_no_active_tasks(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        conductor.request_workflow_state(states.CANCELED)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_set_workflow_canceling_when_has_active_tasks(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        self.forward_task_states(conductor, task_name, [states.RUNNING])

        conductor.request_workflow_state(states.CANCELING)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELING)

    def test_set_workflow_canceled_when_has_active_tasks(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        self.forward_task_states(conductor, task_name, [states.RUNNING])

        conductor.request_workflow_state(states.CANCELED)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELING)

    def test_set_workflow_pausing_when_no_active_tasks(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        conductor.request_workflow_state(states.PAUSING)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

    def test_set_workflow_paused_when_no_active_tasks(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])

        conductor.request_workflow_state(states.PAUSED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

    def test_set_workflow_pausing_when_has_active_tasks(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        self.forward_task_states(conductor, task_name, [states.RUNNING])

        conductor.request_workflow_state(states.PAUSING)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSING)

    def test_set_workflow_paused_when_has_active_tasks(self):
        inputs = {'a': 123}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        task_name = 'task1'
        self.forward_task_states(conductor, task_name, [states.RUNNING])

        conductor.request_workflow_state(states.PAUSED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSING)

    def test_append_log_entries(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        extra = {'x': 1234}
        conductor.log_entry('info', 'The workflow is running as expected.', data=extra)
        conductor.log_entry('warn', 'The task may be running a little bit slow.', task_id='task1')
        conductor.log_entry('error', 'This is baloney.', task_id='task1')
        conductor.log_error(TypeError('Something is not right.'), task_id='task1')
        conductor.log_errors([KeyError('task1'), ValueError('foobar')], task_id='task1')

        self.assertRaises(
            exc.WorkflowLogEntryError,
            conductor.log_entry,
            'foobar',
            'This is foobar.'
        )

        expected_log_entries = [
            {
                'type': 'info',
                'message': 'The workflow is running as expected.',
                'data': extra
            },
            {
                'type': 'warn',
                'message': 'The task may be running a little bit slow.',
                'task_id': 'task1'
            }
        ]

        expected_errors = [
            {
                'type': 'error',
                'message': 'This is baloney.',
                'task_id': 'task1'
            },
            {
                'type': 'error',
                'message': 'TypeError: Something is not right.',
                'task_id': 'task1'
            },
            {
                'type': 'error',
                'message': "KeyError: 'task1'",
                'task_id': 'task1'
            },
            {
                'type': 'error',
                'message': 'ValueError: foobar',
                'task_id': 'task1'
            }
        ]

        self.assertListEqual(conductor.log, expected_log_entries)
        self.assertListEqual(conductor.errors, expected_errors)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'context': {},
            'input': inputs,
            'output': None,
            'state': states.RUNNING,
            'errors': expected_errors,
            'log': expected_log_entries,
            'flow': {
                'routes': [
                    []
                ],
                'staged': [
                    {'id': 'task1', 'route': 0, 'prev': {}, 'ctxs': [0], 'ready': True}
                ],
                'tasks': {},
                'sequence': [],
                'contexts': [
                    {'srcs': [], 'value': inputs}
                ]
            }
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)
        self.assertListEqual(conductor.log, expected_log_entries)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_append_duplicate_log_entries(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        extra = {'x': 1234}
        task = {'task_id': 'task1', 'route': 0}
        conductor.log_entry('info', 'The workflow is running as expected.', data=extra)
        conductor.log_entry('info', 'The workflow is running as expected.', data=extra)
        conductor.log_entry('warn', 'The task may be running a little bit slow.', **task)
        conductor.log_entry('warn', 'The task may be running a little bit slow.', **task)
        conductor.log_entry('error', 'This is baloney.', **task)
        conductor.log_entry('error', 'This is baloney.', **task)
        conductor.log_error(TypeError('Something is not right.'), **task)
        conductor.log_error(TypeError('Something is not right.'), **task)
        conductor.log_errors([KeyError('task1'), ValueError('foobar')], **task)
        conductor.log_errors([KeyError('task1'), ValueError('foobar')], **task)

        expected_log_entries = [
            {
                'type': 'info',
                'message': 'The workflow is running as expected.',
                'data': extra
            },
            {
                'type': 'warn',
                'message': 'The task may be running a little bit slow.',
                'task_id': 'task1',
                'route': 0
            }
        ]

        expected_errors = [
            {
                'type': 'error',
                'message': 'This is baloney.',
                'task_id': 'task1',
                'route': 0
            },
            {
                'type': 'error',
                'message': 'TypeError: Something is not right.',
                'task_id': 'task1',
                'route': 0
            },
            {
                'type': 'error',
                'message': "KeyError: 'task1'",
                'task_id': 'task1',
                'route': 0
            },
            {
                'type': 'error',
                'message': 'ValueError: foobar',
                'task_id': 'task1',
                'route': 0
            }
        ]

        self.assertListEqual(conductor.log, expected_log_entries)
        self.assertListEqual(conductor.errors, expected_errors)
