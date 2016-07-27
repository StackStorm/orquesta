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

from orchestra import states
from orchestra.tests.unit import base


class BasicWorkflowConductorTest(base.WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'direct'
        super(BasicWorkflowConductorTest, cls).setUpClass()

    def test_on_error(self):
        wf_name = 'task-on-error'

        expected_wf_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 'task1'
                },
                {
                    'id': 'task2'
                },
                {
                    'id': 'task3'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-error'
                        )
                    }
                ],
                [],
                []
            ],
            'multigraph': True
        }

        self._assert_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 'task1',
                    'name': 'task1'
                },
                {
                    'id': 'task2',
                    'name': 'task2'
                },
                {
                    'id': 'task3',
                    'name': 'task3'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-error'
                        )
                    }
                ],
                [],
                []
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        # Mock task1 success
        expected_task_seq = [
            'task1',
            'task2'
        ]

        mock_states = [
            states.SUCCESS,     # task1
            states.SUCCESS  # task2
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            mock_states=mock_states
        )

        # Mock task1 error
        expected_task_seq = [
            'task1',
            'task3'
        ]

        mock_states = [
            states.ERROR,   # task1
            states.SUCCESS  # task3
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            mock_states=mock_states
        )

    def test_on_complete(self):
        wf_name = 'task-on-complete'

        expected_wf_graph = {
            'directed': True,
            'graph': {},
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
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-error'
                        )
                    },
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-complete'
                        )
                    }
                ],
                [],
                [],
                []
            ],
            'multigraph': True
        }

        self._assert_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 'task1',
                    'name': 'task1'
                },
                {
                    'id': 'task2',
                    'name': 'task2'
                },
                {
                    'id': 'task3',
                    'name': 'task3'
                },
                {
                    'id': 'task4',
                    'name': 'task4'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-error'
                        )
                    },
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-complete'
                        )
                    }
                ],
                [],
                [],
                []
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        # Mock task1 success
        expected_task_seq = [
            'task1',
            'task2',
            'task4'
        ]

        mock_states = [
            states.SUCCESS,     # task1
            states.SUCCESS,     # task2
            states.SUCCESS      # task4
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            mock_states=mock_states
        )

        # Mock task1 error
        expected_task_seq = [
            'task1',
            'task3',
            'task4'
        ]

        mock_states = [
            states.ERROR,       # task1
            states.SUCCESS,     # task3
            states.SUCCESS      # task4
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            mock_states=mock_states
        )

    def test_task_transitions_split(self):
        wf_name = 'task-transitions-split'

        expected_wf_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 'task1'
                },
                {
                    'id': 'task2',
                    'splits': ['task2']
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-complete'
                        )
                    },
                    {
                        'id': 'task2',
                        'key': 1,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-error'
                        )
                    },
                    {
                        'id': 'task2',
                        'key': 2,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self._assert_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 'task1',
                    'name': 'task1'
                },
                {
                    'id': 'task2__1',
                    'name': 'task2',
                    'splits': [('task2', 1)]
                },
                {
                    'id': 'task2__2',
                    'name': 'task2',
                    'splits': [('task2', 2)]
                },
                {
                    'id': 'task2__3',
                    'name': 'task2',
                    'splits': [('task2', 3)]
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2__1',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-complete'
                        )
                    },
                    {
                        'id': 'task2__2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-error'
                        )
                    },
                    {
                        'id': 'task2__3',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [],
                []
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        # Mock task1 success
        expected_task_seq = [
            'task1',
            'task2__1',
            'task2__3'
        ]

        mock_states = [
            states.SUCCESS,     # task1
            states.SUCCESS,     # task2__1 on-complete
            states.SUCCESS      # task2__3 on-success
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            mock_states=mock_states
        )

        # Mock task1 error
        expected_task_seq = [
            'task1',
            'task2__1',
            'task2__2'
        ]

        mock_states = [
            states.ERROR,       # task1
            states.SUCCESS,     # task2__1 on-complete
            states.SUCCESS      # task2__2 on-error
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            mock_states=mock_states
        )
