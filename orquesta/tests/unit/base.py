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

import abc
import copy
import six
from six.moves import queue
import unittest

from orquesta import conducting
from orquesta import events
from orquesta.expressions import base as expressions
from orquesta.specs import loader as specs_loader
from orquesta import states
from orquesta.tests.fixtures import loader as fixture_loader
from orquesta.utils import context as ctx
from orquesta.utils import plugin
from orquesta.utils import specs


@six.add_metaclass(abc.ABCMeta)
class ExpressionEvaluatorTest(unittest.TestCase):
    language = None
    evaluator = None

    @classmethod
    def setUpClass(cls):
        cls.evaluator = plugin.get_module('orquesta.expressions.evaluators', cls.language)


@six.add_metaclass(abc.ABCMeta)
class ExpressionFacadeEvaluatorTest(unittest.TestCase):

    def validate(self, expr):
        return expressions.validate(expr).get('errors', [])


@six.add_metaclass(abc.ABCMeta)
class WorkflowGraphTest(unittest.TestCase):

    def _zip_wf_graph_meta(self, wf_graph_json):
        wf_graph_json['adjacency'] = [
            sorted(link, key=lambda x: x['id']) if link else link
            for link in wf_graph_json['adjacency']
        ]

        wf_graph_meta = sorted(
            zip(wf_graph_json['nodes'], wf_graph_json['adjacency']),
            key=lambda x: x[0]['id']
        )

        return wf_graph_meta

    def assert_graph_equal(self, wf_graph, expected_wf_graph):
        wf_graph_json = wf_graph.serialize()
        wf_graph_meta = self._zip_wf_graph_meta(wf_graph_json)
        expected_wf_graph_meta = self._zip_wf_graph_meta(expected_wf_graph)

        self.assertListEqual(wf_graph_meta, expected_wf_graph_meta)


class WorkflowSpecTest(unittest.TestCase):
    spec_module_name = 'mock'

    def __init__(self, *args, **kwargs):
        super(WorkflowSpecTest, self).__init__(*args, **kwargs)
        self.maxDiff = None

    def get_fixture_path(self, wf_name):
        return self.spec_module_name + '/' + wf_name + '.yaml'

    def get_wf_def(self, wf_name, raw=False):
        return fixture_loader.get_fixture_content(
            self.get_fixture_path(wf_name),
            'workflows',
            raw=raw
        )

    def get_wf_spec(self, wf_name):
        wf_def = self.get_wf_def(wf_name)
        wf_spec = specs.instantiate(self.spec_module_name, wf_def)
        return wf_spec

    def instantiate(self, wf_def):
        return specs.instantiate(self.spec_module_name, wf_def)

    def assert_spec_inspection(self, wf_name, errors=None):
        wf_spec = self.get_wf_spec(wf_name)

        self.assertDictEqual(wf_spec.inspect(), errors or {})


@six.add_metaclass(abc.ABCMeta)
class WorkflowComposerTest(WorkflowGraphTest, WorkflowSpecTest):
    composer = None

    @classmethod
    def setUpClass(cls):
        WorkflowGraphTest.setUpClass()
        WorkflowSpecTest.setUpClass()

        cls.composer = plugin.get_module('orquesta.composers', cls.spec_module_name)
        cls.spec_module = specs_loader.get_spec_module(cls.spec_module_name)
        cls.wf_spec_type = cls.spec_module.WorkflowSpec

    def compose_wf_graph(self, wf_name):
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)

        return self.composer._compose_wf_graph(wf_spec)

    def assert_compose_to_wf_graph(self, wf_name, expected_wf_graph):
        wf_graph = self.compose_wf_graph(wf_name)

        self.assert_graph_equal(wf_graph, expected_wf_graph)

    def compose_wf_ex_graph(self, wf_name):
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)

        return self.composer.compose(wf_spec)

    def assert_compose_to_wf_ex_graph(self, wf_name, expected_wf_ex_graph):
        wf_ex_graph = self.compose_wf_ex_graph(wf_name)

        self.assert_graph_equal(wf_ex_graph, expected_wf_ex_graph)


@six.add_metaclass(abc.ABCMeta)
class WorkflowConductorTest(WorkflowComposerTest):

    def format_task_item(self, task_name, task_init_ctx, task_spec,
                         action_specs=None, task_id=None, task_delay=None,
                         items_count=None, items_concurrency=None):

        if not action_specs and items_count is None:
            action_specs = [
                {
                    'action': task_spec.action,
                    'input': task_spec.input
                }
            ]

        task = {
            'id': task_id or task_name,
            'name': task_name,
            'ctx': task_init_ctx,
            'spec': task_spec,
            'actions': action_specs or []
        }

        if task_delay:
            task['delay'] = task_delay

        if items_count is not None:
            task['items_count'] = items_count
            task['concurrency'] = items_concurrency

        return task

    # The conductor.get_next_tasks make copies of the task specs and render expressions
    # in the task action and task input. So comparing the task specs will not match. In
    # order to match in unit tests. This method is used to serialize the task specs and
    # compare the lists.
    def assert_task_list(self, actual, expected):
        actual_copy = copy.deepcopy(actual)
        expected_copy = copy.deepcopy(expected)

        for task in actual_copy:
            task['spec'] = task['spec'].serialize()

        for task in expected_copy:
            task['ctx']['__current_task'] = {'id': task['id'], 'name': task['name']}
            task['spec'] = task['spec'].serialize()

        self.assertListEqual(actual_copy, expected_copy)

    def assert_conducting_sequences(self, wf_name, expected_task_seq, inputs=None,
                                    mock_states=None, mock_results=None,
                                    expected_workflow_state=None, expected_output=None):
        if inputs is None:
            inputs = {}

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        conductor = conducting.WorkflowConductor(wf_spec, inputs=inputs)
        conductor.request_workflow_state(states.RUNNING)

        context = {}
        q = queue.Queue()
        state_q = queue.Queue()
        result_q = queue.Queue()

        if mock_states:
            for item in mock_states:
                state_q.put(item)

        if mock_results:
            for item in mock_results:
                result_q.put(item)

        # Get start tasks and being conducting workflow.
        for task in conductor.get_next_tasks():
            q.put(task)

        # Serialize workflow conductor to mock async execution.
        wf_conducting_state = conductor.serialize()

        while not q.empty():
            current_task = q.get()
            current_task_id = current_task['id']

            # Deserialize workflow conductor to mock async execution.
            conductor = conducting.WorkflowConductor.deserialize(wf_conducting_state)

            # Set task state to running.
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING)
            conductor.update_task_flow(current_task_id, ac_ex_event)

            # Set current task in context.
            context = ctx.set_current_task(context, current_task)

            # Mock completion of the task.
            state = state_q.get() if not state_q.empty() else states.SUCCEEDED
            result = result_q.get() if not result_q.empty() else None
            ac_ex_event = events.ActionExecutionEvent(state, result=result)
            conductor.update_task_flow(current_task_id, ac_ex_event)

            # Identify the next set of tasks.
            next_tasks = conductor.get_next_tasks(current_task_id)

            for next_task in next_tasks:
                q.put(next_task)

            # Serialize workflow execution graph to mock async execution.
            wf_conducting_state = conductor.serialize()

        self.assertListEqual(expected_task_seq, [entry['id'] for entry in conductor.flow.sequence])

        if expected_workflow_state is not None:
            self.assertEqual(conductor.get_workflow_state(), expected_workflow_state)

        if expected_output is not None:
            self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def assert_workflow_state(self, wf_name, mock_flow, expected_wf_states, conductor=None):
        if not conductor:
            wf_def = self.get_wf_def(wf_name)
            wf_spec = self.spec_module.instantiate(wf_def)
            conductor = conducting.WorkflowConductor(wf_spec)
            conductor.request_workflow_state(states.RUNNING)

        for task_flow_entry, expected_wf_state in zip(mock_flow, expected_wf_states):
            task_id = task_flow_entry['id']
            task_state = task_flow_entry['state']
            ac_ex_event = events.ActionExecutionEvent(task_state)
            conductor.update_task_flow(task_id, ac_ex_event)

            err_ctx = (
                'Workflow state "%s" is not the expected state "%s". '
                'Updated task "%s" with state "%s".' %
                (conductor.get_workflow_state(), expected_wf_state, task_id, task_state)
            )

            self.assertEqual(conductor.get_workflow_state(), expected_wf_state, err_ctx)

        return conductor


class WorkflowConductorWithItemsTest(WorkflowConductorTest):

    def assert_task_items(self, conductor, task_id, task_ctx, items, action_specs,
                          mock_ac_ex_states, expected_task_states, expected_workflow_states,
                          concurrency=None):

        # Set up test cases.
        tests = list(zip(mock_ac_ex_states, expected_task_states, expected_workflow_states))

        # Verify the first set of action executions.
        expected_task = self.format_task_item(
            task_id,
            task_ctx,
            conductor.spec.tasks.get_task(task_id),
            action_specs=action_specs[0:concurrency],
            items_count=len(items),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # If items is an empty list, then mark the task as running.
        if len(items) == 0:
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING)
            conductor.update_task_flow(task_id, ac_ex_event)
        else:
            # Mark the first set of action executions as running.
            for i in range(0, min(len(tests), concurrency or len(items))):
                context = {'item_id': i}
                ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
                conductor.update_task_flow(task_id, ac_ex_event)

        # Ensure the actions listed is accurate when getting next tasks again.
        expected_tasks = []
        capacity = len(items)
        next_item_id = len(tests)
        next_action_specs = []

        if concurrency is not None and concurrency > 0:
            items_running = min(len(tests), concurrency or len(items))
            capacity = concurrency - items_running

        if capacity > 0 and next_item_id < len(items):
            next_action_specs = action_specs[next_item_id:next_item_id + capacity]

        if next_action_specs or len(items) == 0:
            expected_task = self.format_task_item(
                task_id,
                task_ctx,
                conductor.spec.tasks.get_task(task_id),
                action_specs=next_action_specs,
                items_count=len(items),
                items_concurrency=concurrency
            )

            expected_tasks = [expected_task]

        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # If items is an empty list, complete the task.
        if len(items) == 0:
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED)
            conductor.update_task_flow(task_id, ac_ex_event)

        # Mock the action execution for each item.
        for i in range(0, len(tests)):
            context = {'item_id': i}
            result = items[i]
            ac_ex_state = tests[i][0]
            ac_ex_event = events.ActionExecutionEvent(ac_ex_state, result, context)
            conductor.update_task_flow(task_id, ac_ex_event)

            expected_task_state = tests[i][1]
            actual_task_state = conductor.get_task_flow_entry(task_id)['state']

            error_message = (
                'Task execution state "%s" does not match "%s" for item %s.' %
                (actual_task_state, expected_task_state, i)
            )

            self.assertEqual(actual_task_state, expected_task_state, error_message)

            expected_workflow_state = tests[i][2]
            actual_workflow_state = conductor.get_workflow_state()

            error_message = (
                'Workflow execution state "%s" does not match "%s" after item %s update.' %
                (actual_workflow_state, expected_workflow_state, i)
            )

            self.assertEqual(actual_workflow_state, expected_workflow_state, error_message)

            # Process next set of action executions only if there are more test cases.
            if i >= len(tests) - 2 or concurrency is None:
                continue

            item_id = i + concurrency
            expected_tasks = []

            if item_id < len(items):
                expected_task = self.format_task_item(
                    task_id,
                    task_ctx,
                    conductor.spec.tasks.get_task(task_id),
                    action_specs=action_specs[item_id:item_id + 1],
                    items_count=len(items),
                    items_concurrency=concurrency
                )

                expected_tasks = [expected_task]

            actual_tasks = conductor.get_next_tasks()
            self.assert_task_list(actual_tasks, expected_tasks)

            for task in actual_tasks:
                task_id = task['id']

                for action in task['actions']:
                    ctx = {'item_id': action['item_id']}
                    ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=ctx)
                    conductor.update_task_flow(task_id, ac_ex_event)
