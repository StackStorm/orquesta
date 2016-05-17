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

import mock
import uuid

from orchestra.tests.unit import base


class SplitWorkflowConductorTest(base.WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'mistral'
        super(SplitWorkflowConductorTest, cls).setUpClass()

    @mock.patch.object(
        uuid,
        'uuid4',
        mock.MagicMock(side_effect=range(101, 200))
    )
    def test_split(self):
        wf_name = 'split'
        sub_wf_graph_name = wf_name + '.task4'

        expected_wf_graphs = {
            wf_name: {
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
                        'id': 'task4',
                        'subgraph': wf_name + '.task4'
                    }
                ],
                'adjacency': [
                    [
                        {
                            'id': 'task2',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task1',
                                'succeeded'
                            )
                        },
                        {
                            'id': 'task3',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task1',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task4',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task2',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task4',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task3',
                                'succeeded'
                            )
                        }
                    ],
                    []
                ],
                'multigraph': True
            },
            sub_wf_graph_name: {
                'directed': True,
                'graph': {},
                'nodes': [
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
                        'id': 'task7',
                        'join': True
                    }
                ],
                'adjacency': [
                    [
                        {
                            'id': 'task5',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task4',
                                'succeeded'
                            )
                        },
                        {
                            'id': 'task6',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task4',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task7',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task5',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task7',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task6',
                                'succeeded'
                            )
                        }
                    ],
                    []
                ],
                'multigraph': True
            }
        }

        self._assert_wf_graphs(wf_name, expected_wf_graphs)

        expected_wf_ex_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': '101',
                    'name': 'task1',
                    'workflow': 'split'
                },
                {
                    'id': '102',
                    'name': 'task2',
                    'workflow': 'split'
                },
                {
                    'id': '103',
                    'name': 'task4',
                    'workflow': 'split'
                },
                {
                    'id': '104',
                    'name': 'task5',
                    'workflow': 'split'
                },
                {
                    'id': '105',
                    'name': 'task7',
                    'workflow': 'split',
                    'join': True
                },
                {
                    'id': '106',
                    'name': 'task6',
                    'workflow': 'split'
                },
                {
                    'id': '107',
                    'name': 'task3',
                    'workflow': 'split'
                },
                {
                    'id': '108',
                    'name': 'task4',
                    'workflow': 'split'
                },
                {
                    'id': '109',
                    'name': 'task5',
                    'workflow': 'split'
                },
                {
                    'id': '110',
                    'name': 'task7',
                    'workflow': 'split',
                    'join': True
                },
                {
                    'id': '111',
                    'name': 'task6',
                    'workflow': 'split'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': '102',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'succeeded'
                        )
                    },
                    {
                        'id': '107',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '103',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task2',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '104',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                    {
                        'id': '106',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                ],
                [
                    {
                        'id': '105',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'succeeded'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': '105',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '108',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task3',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '109',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                    {
                        'id': '111',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                ],
                [
                    {
                        'id': '110',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'succeeded'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': '110',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'succeeded'
                        )
                    }
                ]
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        expected_task_seq = [
            '101',
            '102',
            '107',
            '103',
            '108',
            '104',
            '106',
            '109',
            '111',
            '105',
            '110'
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)

    @mock.patch.object(
        uuid,
        'uuid4',
        mock.MagicMock(side_effect=range(101, 200))
    )
    def test_splits(self):
        wf_name = 'splits'

        expected_wf_graphs = {
            wf_name: {
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
                        'id': 'task4',
                        'subgraph': wf_name + '.task4'
                    },
                    {
                        'id': 'task8',
                        'subgraph': wf_name + '.task8'
                    }
                ],
                'adjacency': [
                    [
                        {
                            'id': 'task2',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task1',
                                'succeeded'
                            )
                        },
                        {
                            'id': 'task3',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task1',
                                'succeeded'
                            )
                        },
                        {
                            'id': 'task8',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task1',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task4',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task2',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task4',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task3',
                                'succeeded'
                            )
                        }
                    ],
                    [],
                    []
                ],
                'multigraph': True
            },
            wf_name + '.task4': {
                'directed': True,
                'graph': {},
                'nodes': [
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
                        'id': 'task7',
                        'join': True
                    },
                    {
                        'id': 'task8',
                        'subgraph': wf_name + '.task8'
                    }
                ],
                'adjacency': [
                    [
                        {
                            'id': 'task5',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task4',
                                'succeeded'
                            )
                        },
                        {
                            'id': 'task6',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task4',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task7',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task5',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task7',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task6',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task8',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task7',
                                'succeeded'
                            )
                        }
                    ],
                    []
                ],
                'multigraph': True
            },
            wf_name + '.task8': {
                'directed': True,
                'graph': {},
                'nodes': [
                    {
                        'id': 'task8'
                    }
                ],
                'adjacency': [
                    []
                ]
            }
        }

        self._assert_wf_graphs(wf_name, expected_wf_graphs)

        expected_wf_ex_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': '101',
                    'name': 'task1',
                    'workflow': 'splits'
                },
                {
                    'id': '102',
                    'name': 'task2',
                    'workflow': 'splits'
                },
                {
                    'id': '103',
                    'name': 'task4',
                    'workflow': 'splits'
                },
                {
                    'id': '104',
                    'name': 'task5',
                    'workflow': 'splits'
                },
                {
                    'id': '105',
                    'name': 'task7',
                    'workflow': 'splits',
                    'join': True
                },
                {
                    'id': '106',
                    'name': 'task8',
                    'workflow': 'splits'
                },
                {
                    'id': '107',
                    'name': 'task6',
                    'workflow': 'splits'
                },
                {
                    'id': '108',
                    'name': 'task3',
                    'workflow': 'splits'
                },
                {
                    'id': '109',
                    'name': 'task4',
                    'workflow': 'splits'
                },
                {
                    'id': '110',
                    'name': 'task5',
                    'workflow': 'splits'
                },
                {
                    'id': '111',
                    'name': 'task7',
                    'workflow': 'splits',
                    'join': True
                },
                {
                    'id': '112',
                    'name': 'task8',
                    'workflow': 'splits'
                },
                {
                    'id': '113',
                    'name': 'task6',
                    'workflow': 'splits'
                },
                {
                    'id': '114',
                    'name': 'task8',
                    'workflow': 'splits'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': '102',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'succeeded'
                        )
                    },
                    {
                        'id': '108',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'succeeded'
                        )
                    },
                    {
                        'id': '114',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '103',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task2',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '104',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                    {
                        'id': '107',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                ],
                [
                    {
                        'id': '105',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '106',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'succeeded'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': '105',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '109',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task3',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '110',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                    {
                        'id': '113',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                ],
                [
                    {
                        'id': '111',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '112',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'succeeded'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': '111',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'succeeded'
                        )
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        expected_task_seq = [
            '101',
            '102',
            '108',
            '114',
            '103',
            '109',
            '104',
            '107',
            '110',
            '113',
            '105',
            '111',
            '106',
            '112'
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)
