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

from orchestra import composition
from orchestra.tests.unit import base


EXPECTED_WF_GRAPH = {
    'directed': True,
    'graph': [],
    'nodes': [
        {
            'id': 'task1'
        },
        {
            'id': 'task2'
        },
        {
            'id': 'task3'
        },
        {
            'id': 'task4'
        },
        {
            'id': 'task5'
        },
        {
            'id': 'task6'
        },
        {
            'id': 'task7'
        },
        {
            'id': 'task8'
        }
    ],
    'adjacency': [
        [
            {
                'id': 'task2',
                'key': 0
            },
            {
                'id': 'task4',
                'key': 0
            },
            {
                'id': 'task7',
                'key': 0
            }
        ],
        [
            {
                'id': 'task3',
                'key': 0
            }
        ],
        [
            {
                'id': 'task5',
                'key': 0
            }
        ],
        [
            {
                'id': 'task5',
                'key': 0
            }
        ],
        [
            {
                'id': 'task6',
                'key': 0
            }
        ],
        [],
        [
            {
                'id': 'task8',
                'key': 0
            }
        ],
        []
    ],
    'multigraph': True
}


class WorkflowGraphTest(base.WorkflowGraphTest):

    def _add_tasks(self, wf_graph):
        for i in range(1, 9):
            wf_graph.add_task('task' + str(i))

    def _add_sequences(self, wf_graph):
        wf_graph.add_sequence('task1', 'task2')
        wf_graph.add_sequence('task2', 'task3')
        wf_graph.add_sequence('task1', 'task4')
        wf_graph.add_sequence('task3', 'task5')
        wf_graph.add_sequence('task4', 'task5')
        wf_graph.add_sequence('task5', 'task6')
        wf_graph.add_sequence('task1', 'task7')
        wf_graph.add_sequence('task7', 'task8')

    def test_basic_graph(self):
        wf_graph = composition.WorkflowGraph()
        self._add_tasks(wf_graph)
        self._add_sequences(wf_graph)

        self._assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_skip_add_tasks(self):
        wf_graph = composition.WorkflowGraph()
        self._add_sequences(wf_graph)

        self._assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_duplicate_add_tasks(self):
        wf_graph = composition.WorkflowGraph()
        self._add_tasks(wf_graph)
        self._add_tasks(wf_graph)
        self._add_sequences(wf_graph)

        self._assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_duplicate_add_sequences(self):
        wf_graph = composition.WorkflowGraph()
        self._add_tasks(wf_graph)
        self._add_sequences(wf_graph)
        self._add_sequences(wf_graph)

        self._assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)
