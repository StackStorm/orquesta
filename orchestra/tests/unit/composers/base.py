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
import six

from orchestra.utils import plugin
from orchestra.tests.fixtures import loader
from orchestra.tests.unit import base


@six.add_metaclass(abc.ABCMeta)
class WorkflowComposerTest(base.WorkflowGraphTest):
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
        return self.composer._compose_wf_graphs(
            self._get_wf_def(wf_name),
            entry=wf_name
        )

    def _assert_wf_graphs(self, wf_name, expected_wf_graphs):
        wf_graphs = self.composer._compose_wf_graphs(
            self._get_wf_def(wf_name),
            entry=wf_name
        )

        self._assert_graph_equal(
            wf_graphs[wf_name],
            expected_wf_graphs[wf_name]
        )

        return wf_graphs

    def _assert_compose(self, wf_name, expected_wf_ex_graph):
        wf_ex_graph = self.composer.compose(
            self._get_wf_def(wf_name),
            entry=wf_name
        )

        self._assert_graph_equal(wf_ex_graph, expected_wf_ex_graph)

        return wf_ex_graph
