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

from orchestra import composition
from orchestra.expressions import base as expressions
from orchestra import states
from orchestra import symphony
from orchestra.utils import plugin
from orchestra.specs import v2 as specs
from orchestra.tests.fixtures import loader


@six.add_metaclass(abc.ABCMeta)
class ExpressionEvaluatorTest(unittest.TestCase):
    language = None
    evaluator = None

    @classmethod
    def setUpClass(cls):
        cls.evaluator = plugin.get_module(
            'orchestra.expressions.evaluators',
            cls.language
        )


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
        wf_graph_meta = self._zip_wf_graph_meta(wf_graph.serialize())
        expected_wf_graph_meta = self._zip_wf_graph_meta(expected_wf_graph)

        self.assertListEqual(wf_graph_meta, expected_wf_graph_meta)


class WorkflowSpecTest(unittest.TestCase):

    def get_fixture_path(self, wf_name, rel_path=None):
        if rel_path:
            return rel_path + '/' + wf_name + '.yaml'
        else:
            return wf_name + '.yaml'

    def get_wf_def(self, wf_name, rel_path=None, raw=False):
        return loader.get_fixture_content(
            self.get_fixture_path(wf_name, rel_path=rel_path),
            'workflows',
            raw=raw
        )


@six.add_metaclass(abc.ABCMeta)
class WorkflowComposerTest(WorkflowGraphTest, WorkflowSpecTest):
    composer_name = None
    composer = None

    @classmethod
    def setUpClass(cls):
        WorkflowGraphTest.setUpClass()
        WorkflowSpecTest.setUpClass()

        cls.composer = plugin.get_module(
            'orchestra.composers',
            cls.composer_name
        )

        cls.wf_spec_type = (
            specs.ReverseWorkflowSpec
            if cls.composer_name == 'reverse'
            else specs.DirectWorkflowSpec
        )

        cls.fixture_rel_path = cls.composer_name

    def compose_seq_expr(self, name, *args, **kwargs):
        return self.composer._compose_sequence_criteria(name, *args, **kwargs)

    def compose_wf_graph(self, wf_name):
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = self.wf_spec_type(wf_def)

        return self.composer._compose_wf_graph(wf_spec)

    def assert_compose_to_wf_graph(self, wf_name, expected_wf_graph):
        wf_graph = self.compose_wf_graph(wf_name)

        self.assert_graph_equal(wf_graph, expected_wf_graph)

    def compose_wf_ex_graph(self, wf_name):
        wf_def = self.get_wf_def(wf_name, rel_path=self.fixture_rel_path)
        wf_spec = self.wf_spec_type(wf_def)

        return self.composer.compose(wf_spec)

    def assert_compose_to_wf_ex_graph(self, wf_name, expected_wf_ex_graph):
        wf_ex_graph = self.compose_wf_ex_graph(wf_name)

        self.assert_graph_equal(wf_ex_graph, expected_wf_ex_graph)


class DirectWorkflowComposerTest(WorkflowComposerTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'direct'
        super(DirectWorkflowComposerTest, cls).setUpClass()


class ReverseWorkflowComposerTest(WorkflowComposerTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'reverse'
        super(ReverseWorkflowComposerTest, cls).setUpClass()


@six.add_metaclass(abc.ABCMeta)
class WorkflowConductorTest(WorkflowComposerTest):

    def assert_conducting_sequences(self, wf_name, expected_task_seq,
                                    mock_contexts=None, mock_states=None):

        wf_ex_graph = self.compose_wf_ex_graph(wf_name)
        wf_ex_graph_json = wf_ex_graph.serialize()

        context = {}
        actual_task_seq = []
        q = queue.Queue()
        ctx_q = queue.Queue()
        state_q = queue.Queue()

        if mock_contexts:
            for item in mock_contexts:
                ctx_q.put(item)

        if mock_states:
            for item in mock_states:
                state_q.put(item)

        wf_ex_graph = composition.WorkflowGraph.deserialize(wf_ex_graph_json)
        conductor = symphony.WorkflowConductor(wf_ex_graph)

        for task in conductor.start_workflow():
            q.put(task)

        # serialize workflow execution graph to mock async execution
        wf_ex_graph_json = wf_ex_graph.serialize()

        while not q.empty():
            queued_task = q.get()

            # mock completion of the task
            state = state_q.get() if not state_q.empty() else states.SUCCESS
            actual_task_seq.append(queued_task['id'])
            completed_task = copy.deepcopy(queued_task)
            completed_task['state'] = state

            # deserialize workflow execution graph to mock async execution
            wf_ex_graph = composition.WorkflowGraph.deserialize(
                wf_ex_graph_json
            )

            # Instantiate a new conductor to mock async execution
            conductor = symphony.WorkflowConductor(wf_ex_graph)

            if not ctx_q.empty():
                context = ctx_q.get()

            next_tasks = conductor.on_task_complete(
                completed_task,
                context=context
            )

            for next_task in next_tasks:
                q.put(next_task)

            # serialize workflow execution graph to mock async execution
            wf_ex_graph_json = wf_ex_graph.serialize()

        self.assertListEqual(expected_task_seq, actual_task_seq)


class DirectWorkflowConductorTest(WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'direct'
        super(DirectWorkflowConductorTest, cls).setUpClass()


class ReverseWorkflowConductorTest(WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'reverse'
        super(ReverseWorkflowConductorTest, cls).setUpClass()
