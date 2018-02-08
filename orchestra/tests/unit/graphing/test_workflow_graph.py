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

from orchestra import graphing
from orchestra import states
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
            'id': 'task5',
            'barrier': '*'
        },
        {
            'id': 'task6'
        },
        {
            'id': 'task7'
        },
        {
            'id': 'task8'
        },
        {
            'id': 'task9'
        }
    ],
    'adjacency': [
        [
            {
                'id': 'task2',
                'key': 0,
                'criteria': None
            },
            {
                'id': 'task4',
                'key': 0,
                'criteria': None
            },
            {
                'id': 'task7',
                'key': 0,
                'criteria': None
            },
            {
                'id': 'task9',
                'key': 0,
                'criteria': None
            }
        ],
        [
            {
                'id': 'task3',
                'key': 0,
                'criteria': None
            }
        ],
        [
            {
                'id': 'task5',
                'key': 0,
                'criteria': None
            }
        ],
        [
            {
                'id': 'task5',
                'key': 0,
                'criteria': None
            }
        ],
        [
            {
                'id': 'task6',
                'key': 0,
                'criteria': None
            }
        ],
        [],
        [
            {
                'id': 'task8',
                'key': 0,
                'criteria': None
            }
        ],
        [
            {
                'id': 'task9',
                'key': 0,
                'criteria': None
            }
        ],
        []
    ],
    'multigraph': True
}


class WorkflowGraphTest(base.WorkflowGraphTest):

    def _add_tasks(self, wf_graph):
        for i in range(1, 10):
            wf_graph.add_task('task' + str(i))

    def _add_transitions(self, wf_graph):
        wf_graph.add_transition('task1', 'task2')
        wf_graph.add_transition('task2', 'task3')
        wf_graph.add_transition('task1', 'task4')
        wf_graph.add_transition('task3', 'task5')
        wf_graph.add_transition('task4', 'task5')
        wf_graph.add_transition('task5', 'task6')
        wf_graph.add_transition('task1', 'task7')
        wf_graph.add_transition('task7', 'task8')
        wf_graph.add_transition('task1', 'task9')
        wf_graph.add_transition('task8', 'task9')

    def _add_barriers(self, wf_graph):
        wf_graph.update_task('task5', barrier='*')

    def _prep_graph(self, wf_graph):
        self._add_tasks(wf_graph)
        self._add_transitions(wf_graph)
        self._add_barriers(wf_graph)

    def test_basic_graph(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        self.assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_get_set_graph_state(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)
        expected_wf_graph = copy.deepcopy(EXPECTED_WF_GRAPH)

        self.assert_graph_equal(wf_graph, expected_wf_graph)
        self.assertIsNone(wf_graph.state)

        wf_graph.state = states.RUNNING
        expected_wf_graph['graph'] = [('state', states.RUNNING)]

        self.assert_graph_equal(wf_graph, expected_wf_graph)
        self.assertEqual(wf_graph.state, states.RUNNING)

    def test_get_set_bad_graph_state(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        self.assertRaises(ValueError, wf_graph.state, 'foobar')
        self.assertIsNone(wf_graph.state)
        self.assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_skip_add_tasks(self):
        wf_graph = graphing.WorkflowGraph()
        self._add_transitions(wf_graph)
        self._add_barriers(wf_graph)

        self.assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_duplicate_add_tasks(self):
        wf_graph = graphing.WorkflowGraph()
        self._add_tasks(wf_graph)
        self._prep_graph(wf_graph)

        self.assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_duplicate_add_transitions(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)
        self._add_transitions(wf_graph)

        self.assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_get_start_tasks(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        expected_start_tasks = [{'id': 'task1', 'name': 'task1'}]

        self.assertListEqual(wf_graph.get_start_tasks(), expected_start_tasks)

    def test_get_next_transitions(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        expected_transitions = [
            ('task1', 'task2', 0, {'criteria': None}),
            ('task1', 'task4', 0, {'criteria': None}),
            ('task1', 'task7', 0, {'criteria': None}),
            ('task1', 'task9', 0, {'criteria': None})
        ]

        self.assertListEqual(
            sorted(wf_graph.get_next_transitions('task1')),
            sorted(expected_transitions)
        )

    def test_get_prev_transitions(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        expected_transitions = [
            ('task3', 'task5', 0, {'criteria': None}),
            ('task4', 'task5', 0, {'criteria': None})
        ]

        self.assertListEqual(
            sorted(wf_graph.get_prev_transitions('task5')),
            sorted(expected_transitions)
        )

    def test_barrier_task(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        self.assertTrue(wf_graph.has_barrier('task5'))
        self.assertFalse(wf_graph.has_barrier('task9'))

    def test_split_from_reused_task(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        self.assertFalse(
            len(wf_graph.get_prev_transitions('task5')) > 1 and
            not wf_graph.has_barrier('task5')
        )

        self.assertTrue(
            len(wf_graph.get_prev_transitions('task9')) > 1 and
            not wf_graph.has_barrier('task9')
        )

    def test_get_task_attributes(self):
        wf_graph = graphing.WorkflowGraph()
        self._prep_graph(wf_graph)

        wf_graph.update_task('task1', state=states.RUNNING)
        wf_graph.update_task('task2', state=states.RUNNING)
        wf_graph.update_task('task4', state=states.RUNNING)

        expected = {
            'task1': states.RUNNING,
            'task2': states.RUNNING,
            'task3': None,
            'task4': states.RUNNING,
            'task5': None,
            'task6': None,
            'task7': None,
            'task8': None,
            'task9': None
        }

        self.assertDictEqual(wf_graph.get_task_attributes('state'), expected)
