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


class JoinWorkflowConductorTest(base.WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'mistral'
        super(JoinWorkflowConductorTest, cls).setUpClass()

    def test_join(self):
        wf_name = 'join'

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
                },
                {
                    'id': 'task5'
                },
                {
                    'id': 'task6',
                    'join': 'all'
                },
                {
                    'id': 'task7'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
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
                },
                {
                    'id': 'task5',
                    'name': 'task5'
                },
                {
                    'id': 'task6',
                    'name': 'task6',
                    'join': 'all'
                },
                {
                    'id': 'task7',
                    'name': 'task7'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task3',
            'task5',
            'task6',
            'task7'
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)

    def test_join_count(self):
        wf_name = 'join-count'

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
                    'id': 'task8',
                    'join': 2
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
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
                },
                {
                    'id': 'task5',
                    'name': 'task5'
                },
                {
                    'id': 'task6',
                    'name': 'task6'
                },
                {
                    'id': 'task7',
                    'name': 'task7'
                },
                {
                    'id': 'task8',
                    'name': 'task8',
                    'join': 2
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        # Mock error at task6
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task3',
            'task5',
            'task8'
        ]

        mock_states = [
            states.SUCCESS,     # task1
            states.SUCCESS,     # task2
            states.SUCCESS,     # task4
            states.ERROR,       # task6
            states.SUCCESS,     # task3
            states.SUCCESS,     # task5
            states.SUCCESS      # task8
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            mock_states=mock_states
        )

        # Mock error at task7
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task3',
            'task5',
            'task7',
            'task8'
        ]

        mock_states = [
            states.SUCCESS,     # task1
            states.SUCCESS,     # task2
            states.SUCCESS,     # task4
            states.SUCCESS,     # task6
            states.SUCCESS,     # task3
            states.SUCCESS,     # task5
            states.ERROR,       # task7
            states.SUCCESS      # task8
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            mock_states=mock_states
        )
