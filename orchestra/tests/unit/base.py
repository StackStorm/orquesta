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
from orchestra import specs
from orchestra import states
from orchestra import symphony
from orchestra.utils import plugin
from orchestra.tests.fixtures import loader


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

    def _assert_graph_equal(self, wf_graph, expected_wf_graph):
        wf_graph_meta = self._zip_wf_graph_meta(wf_graph.serialize())
        expected_wf_graph_meta = self._zip_wf_graph_meta(expected_wf_graph)

        self.assertListEqual(wf_graph_meta, expected_wf_graph_meta)


@six.add_metaclass(abc.ABCMeta)
class WorkflowConductorTest(WorkflowGraphTest):
    composer_name = None
    composer = None

    @classmethod
    def setUpClass(cls):
        cls.composer = plugin.get_module(
            'orchestra.composers',
            cls.composer_name
        )

    def _get_fixture_path(self, wf_name):
        return self.composer_name + '/' + wf_name + '.yaml'

    def _get_wf_def(self, wf_name):
        return loader.get_fixture_content(
            self._get_fixture_path(wf_name),
            'workflows'
        )

    def _serialize_wf_graphs(self, wf_graphs):
        return {
            name: wf_graph.serialize()
            for name, wf_graph in six.iteritems(wf_graphs)
        }

    def _get_seq_expr(self, name, state, expr=None):
        return self.composer._compose_task_transition_criteria(
            name,
            state,
            expr=expr
        )

    def _compose_wf_graphs(self, wf_name):
        wf_spec = specs.WorkflowSpec(self._get_wf_def(wf_name))
        return self.composer._compose_wf_graphs(wf_spec)

    def _assert_wf_graphs(self, wf_name, expected_wf_graphs):
        wf_spec = specs.WorkflowSpec(self._get_wf_def(wf_name))
        wf_graphs = self.composer._compose_wf_graphs(wf_spec)

        for name in sorted(wf_graphs.keys()):
            self._assert_graph_equal(
                wf_graphs[name],
                expected_wf_graphs[name]
            )

        return wf_graphs

    def _assert_compose(self, wf_name, expected_wf_ex_graph):
        wf_ex_graph = self.composer.compose(self._get_wf_def(wf_name))

        self._assert_graph_equal(wf_ex_graph, expected_wf_ex_graph)

        return wf_ex_graph

    def _assert_conduct(self, wf_ex_graph_json, expected_task_seq,
                        contexts=None):
        context = {}
        actual_task_seq = []
        q = queue.Queue()
        ctx_q = queue.Queue()

        if contexts:
            for item in contexts:
                ctx_q.put(item)

        wf_ex_graph = composition.WorkflowGraph.deserialize(wf_ex_graph_json)
        conductor = symphony.WorkflowConductor(wf_ex_graph)

        for task_id, attributes in six.iteritems(conductor.start_workflow()):
            attributes['id'] = task_id
            q.put(attributes)

        # serialize workflow execution graph to mock async execution
        wf_ex_graph_json = wf_ex_graph.serialize()

        while not q.empty():
            queued_task = q.get()

            # mock completion of the task
            actual_task_seq.append(queued_task['id'])
            completed_task = copy.deepcopy(queued_task)
            completed_task['state'] = states.SUCCESS

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
