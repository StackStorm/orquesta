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

from orchestra import conducting
from orchestra.expressions import base as expressions
from orchestra.specs import loader as specs_loader
from orchestra import states
from orchestra.tests.fixtures import loader as fixture_loader
from orchestra.utils import context as ctx
from orchestra.utils import plugin
from orchestra.utils import specs


@six.add_metaclass(abc.ABCMeta)
class ExpressionEvaluatorTest(unittest.TestCase):
    language = None
    evaluator = None

    @classmethod
    def setUpClass(cls):
        cls.evaluator = plugin.get_module('orchestra.expressions.evaluators', cls.language)


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

        cls.composer = plugin.get_module('orchestra.composers', cls.spec_module_name)
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

    def format_task_item(self, task_name, task_init_ctx, task_spec, task_id=None):
        return {
            'id': task_id or task_name,
            'name': task_name,
            'ctx': task_init_ctx,
            'spec': task_spec
        }

    # The conductor.get_start_tasks and conductor.get_next_tasks make copies of the
    # task specs and render expressions in the task action and task input. So comparing
    # the task specs will not match. In order to match in unit tests. This method is
    # used to serialize the task specs and compare the lists.
    def assert_task_list(self, actual, expected):
        actual_copy = copy.deepcopy(actual)
        expected_copy = copy.deepcopy(expected)

        for task in actual_copy:
            task['spec'] = task['spec'].serialize()

        for task in expected_copy:
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
        conductor.set_workflow_state(states.RUNNING)

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
        for task in conductor.get_start_tasks():
            q.put(task)

        # Serialize workflow conductor to mock async execution.
        wf_conducting_state = conductor.serialize()

        while not q.empty():
            current_task = q.get()
            current_task_id = current_task['id']

            # Deserialize workflow conductor to mock async execution.
            conductor = conducting.WorkflowConductor.deserialize(wf_conducting_state)

            # Set task state to running.
            conductor.update_task_flow(current_task_id, states.RUNNING)

            # Set current task in context.
            context = ctx.set_current_task(context, current_task)

            # Mock completion of the task.
            state = state_q.get() if not state_q.empty() else states.SUCCEEDED
            result = result_q.get() if not result_q.empty() else None
            conductor.update_task_flow(current_task_id, state, result=result)

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

    def assert_workflow_state(self, wf_name, mock_flow_entries, expected_wf_states):
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        conductor = conducting.WorkflowConductor(wf_spec)
        conductor.set_workflow_state(states.RUNNING)

        for task_flow_entry, expected_wf_state in zip(mock_flow_entries, expected_wf_states):
            task_id = task_flow_entry['id']
            task_state = task_flow_entry['state']
            conductor.update_task_flow(task_id, task_state)

            err_ctx = (
                'Workflow state "%s" is not the expected state "%s". '
                'Updated task "%s" with state "%s".' %
                (conductor.get_workflow_state(), expected_wf_state, task_id, task_state)
            )

            self.assertEqual(conductor.get_workflow_state(), expected_wf_state, err_ctx)
