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
            '101',  # task1
            '102',  # task2
            '107',  # task3
            '103',  # task4.1
            '108',  # task4.2
            '104',  # task5.1
            '106',  # task6.1
            '109',  # task5.2
            '111',  # task6.2
            '105',  # task7.1
            '110'   # task7.2
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
            '101',  # task1
            '102',  # task2
            '108',  # task3
            '114',  # task8
            '103',  # task4.1
            '109',  # task4.2
            '104',  # task5.1
            '107',  # task6.1
            '110',  # task5.2
            '113',  # task6.2
            '105',  # task7.1
            '111',  # task7.2
            '106',  # task8.1
            '112'   # task8.2
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)

    @mock.patch.object(
        uuid,
        'uuid4',
        mock.MagicMock(side_effect=range(101, 200))
    )
    def test_nested_splits(self):
        wf_name = 'splits-nested'

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
                        'subgraph': wf_name + '.task7'
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
            },
            wf_name + '.task7': {
                'directed': True,
                'graph': {},
                'nodes': [
                    {
                        'id': 'task7'
                    },
                    {
                        'id': 'task8'
                    },
                    {
                        'id': 'task9'
                    },
                    {
                        'id': 'task10',
                        'join': True
                    }
                ],
                'adjacency': [
                   [
                        {
                            'id': 'task8',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task7',
                                'succeeded'
                            )
                        },
                        {
                            'id': 'task9',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task7',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task10',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task8',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task10',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task9',
                                'succeeded'
                            )
                        }
                    ],
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
                    'workflow': 'splits-nested'
                },
                {
                    'id': '102',
                    'name': 'task2',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '103',
                    'name': 'task4',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '104',
                    'name': 'task5',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '105',
                    'name': 'task7',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '106',
                    'name': 'task8',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '107',
                    'name': 'task10',
                    'workflow': 'splits-nested',
                    'join': True
                },
                {
                    'id': '108',
                    'name': 'task9',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '109',
                    'name': 'task6',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '110',
                    'name': 'task7',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '111',
                    'name': 'task8',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '112',
                    'name': 'task10',
                    'workflow': 'splits-nested',
                    'join': True
                },
                {
                    'id': '113',
                    'name': 'task9',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '114',
                    'name': 'task3',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '115',
                    'name': 'task4',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '116',
                    'name': 'task5',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '117',
                    'name': 'task7',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '118',
                    'name': 'task8',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '119',
                    'name': 'task10',
                    'workflow': 'splits-nested',
                    'join': True
                },
                {
                    'id': '120',
                    'name': 'task9',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '121',
                    'name': 'task6',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '122',
                    'name': 'task7',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '123',
                    'name': 'task8',
                    'workflow': 'splits-nested'
                },
                {
                    'id': '124',
                    'name': 'task10',
                    'workflow': 'splits-nested',
                    'join': True
                },
                {
                    'id': '125',
                    'name': 'task9',
                    'workflow': 'splits-nested'
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
                        'id': '109',
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
                    },
                    {
                        'id': '108',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '107',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task8',
                            'succeeded'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': '107',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task9',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '110',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '111',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'succeeded'
                        )
                    },
                    {
                        'id': '113',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '112',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task8',
                            'succeeded'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': '112',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task9',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '115',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task3',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '116',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                    {
                        'id': '121',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    },
                ],
                [
                    {
                        'id': '117',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '118',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'succeeded'
                        )
                    },
                    {
                        'id': '120',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '119',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task8',
                            'succeeded'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': '119',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task9',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '122',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '123',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'succeeded'
                        )
                    },
                    {
                        'id': '125',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task7',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '124',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task8',
                            'succeeded'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': '124',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task9',
                            'succeeded'
                        )
                    }
                ]
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        expected_task_seq = [
            '101',  # task1
            '102',  # task2
            '114',  # task3
            '103',  # task4.1
            '115',  # task4.2
            '104',  # task5.1
            '109',  # task6.1
            '116',  # task5.2
            '121',  # task6.2
            '105',  # task7.1.1
            '110',  # task7.1.2
            '117',  # task7.2.1
            '122',  # task7.2.2
            '106',  # task8.1.1
            '108',  # task9.1.1
            '111',  # task8.1.2
            '113',  # task9.1.2
            '118',  # task8.2.1
            '120',  # task9.2.1
            '123',  # task8.2.2
            '125',  # task9.2.2
            '107',  # task10.1.1
            '112',  # task10.1.2
            '119',  # task10.2.1
            '124',  # task10.2.2
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)
