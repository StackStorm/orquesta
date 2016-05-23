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


class SplitWorkflowConductorTest(base.WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'mistral'
        super(SplitWorkflowConductorTest, cls).setUpClass()

    def test_split(self):
        wf_name = 'split'

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
                    'id': 'task4',
                    'splits': ['task4']
                },
                {
                    'id': 'task5',
                    'splits': ['task4']
                },
                {
                    'id': 'task6',
                    'splits': ['task4']
                },
                {
                    'id': 'task7',
                    'join': 'all',
                    'splits': ['task4']
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': False
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
                    'id': 'task4__1',
                    'name': 'task4',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task5__1',
                    'name': 'task5',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task7__1',
                    'name': 'task7',
                    'join': 'all',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task6__1',
                    'name': 'task6',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task3',
                    'name': 'task3'
                },
                {
                    'id': 'task4__2',
                    'name': 'task4',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task5__2',
                    'name': 'task5',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task7__2',
                    'name': 'task7',
                    'join': 'all',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task6__2',
                    'name': 'task6',
                    'splits': [('task4', 2)]
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__1',
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__1',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6__1',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__1',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__1',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__2',
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__2',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6__2',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__2',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__2',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ]
            ],
            'multigraph': False
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        expected_task_seq = [
            'task1',
            'task2',
            'task3',
            'task4__1',
            'task4__2',
            'task5__1',
            'task6__1',
            'task5__2',
            'task6__2',
            'task7__1',
            'task7__2'
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)

    def test_splits(self):
        wf_name = 'splits'

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
                    'id': 'task4',
                    'splits': ['task4']
                },   
                {    
                    'id': 'task5',
                    'splits': ['task4']
                },   
                {    
                    'id': 'task6',
                    'splits': ['task4']
                },
                {
                    'id': 'task7',
                    'join': 'all',
                    'splits': ['task4']
                },
                {
                    'id': 'task8',
                    'splits': ['task4', 'task8']
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task8',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': False
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
                    'id': 'task4__1',
                    'name': 'task4',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task5__1',
                    'name': 'task5',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task7__1',
                    'name': 'task7',
                    'join': 'all',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task8__2',
                    'name': 'task8',
                    'splits': [('task4', 1), ('task8', 2)]
                },
                {
                    'id': 'task6__1',
                    'name': 'task6',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task3',
                    'name': 'task3'
                },
                {
                    'id': 'task4__2',
                    'name': 'task4',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task5__2',
                    'name': 'task5',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task7__2',
                    'name': 'task7',
                    'join': 'all',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task8__3',
                    'name': 'task8',
                    'splits': [('task4', 2), ('task8', 3)]
                },
                {
                    'id': 'task6__2',
                    'name': 'task6',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task8__1',
                    'name': 'task8',
                    'splits': [('task8', 1)]
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task8__1',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__1',
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__1',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6__1',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__1',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__2',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__1',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__2',
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__2',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6__2',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__2',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__3',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__2',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': False
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        expected_task_seq = [
            'task1',
            'task2',
            'task3',
            'task8__1',
            'task4__1',
            'task4__2',
            'task5__1',
            'task6__1',
            'task5__2',
            'task6__2',
            'task7__1',
            'task7__2',
            'task8__2',
            'task8__3'
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)

    def test_nested_splits(self):
        wf_name = 'splits-nested'

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
                    'id': 'task4',
                    'splits': ['task4']
                },   
                {    
                    'id': 'task5',
                    'splits': ['task4']
                },   
                {    
                    'id': 'task6',
                    'splits': ['task4']
                },
                {
                    'id': 'task7',
                    'splits': ['task4', 'task7']
                },
                {
                    'id': 'task8',
                    'splits': ['task4', 'task7']
                },
                {
                    'id': 'task9',
                    'splits': ['task4', 'task7']
                },
                {
                    'id': 'task10',
                    'join': 'all',
                    'splits': ['task4', 'task7']
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task9',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10',
                        'criteria': self._get_seq_expr(
                            'task8',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10',
                        'criteria': self._get_seq_expr(
                            'task9',
                            'on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': False
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
                    'id': 'task4__1',
                    'name': 'task4',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task5__1',
                    'name': 'task5',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task7__1',
                    'name': 'task7',
                    'splits': [('task4', 1), ('task7', 1)]
                },
                {
                    'id': 'task8__1',
                    'name': 'task8',
                    'splits': [('task4', 1), ('task7', 1)]
                },
                {
                    'id': 'task10__1',
                    'name': 'task10',
                    'join': 'all',
                    'splits': [('task4', 1), ('task7', 1)]
                },
                {
                    'id': 'task9__1',
                    'name': 'task9',
                    'splits': [('task4', 1), ('task7', 1)]
                },
                {
                    'id': 'task6__1',
                    'name': 'task6',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task7__2',
                    'name': 'task7',
                    'splits': [('task4', 1), ('task7', 2)]
                },
                {
                    'id': 'task8__2',
                    'name': 'task8',
                    'splits': [('task4', 1), ('task7', 2)]
                },
                {
                    'id': 'task10__2',
                    'name': 'task10',
                    'join': 'all',
                    'splits': [('task4', 1), ('task7', 2)]
                },
                {
                    'id': 'task9__2',
                    'name': 'task9',
                    'splits': [('task4', 1), ('task7', 2)]
                },
                {
                    'id': 'task3',
                    'name': 'task3'
                },
                {
                    'id': 'task4__2',
                    'name': 'task4',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task5__2',
                    'name': 'task5',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task7__3',
                    'name': 'task7',
                    'splits': [('task4', 2), ('task7', 3)]
                },
                {
                    'id': 'task8__3',
                    'name': 'task8',
                    'splits': [('task4', 2), ('task7', 3)]
                },
                {
                    'id': 'task10__3',
                    'name': 'task10',
                    'join': 'all',
                    'splits': [('task4', 2), ('task7', 3)]
                },
                {
                    'id': 'task9__3',
                    'name': 'task9',
                    'splits': [('task4', 2), ('task7', 3)]
                },
                {
                    'id': 'task6__2',
                    'name': 'task6',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task7__4',
                    'name': 'task7',
                    'splits': [('task4', 2), ('task7', 4)]
                },
                {
                    'id': 'task8__4',
                    'name': 'task8',
                    'splits': [('task4', 2), ('task7', 4)]
                },
                {
                    'id': 'task10__4',
                    'name': 'task10',
                    'join': 'all',
                    'splits': [('task4', 2), ('task7', 4)]
                },
                {
                    'id': 'task9__4',
                    'name': 'task9',
                    'splits': [('task4', 2), ('task7', 4)]
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__1',
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__1',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6__1',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__1',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__1',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task9__1',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10__1',
                        'criteria': self._get_seq_expr(
                            'task8',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task10__1',
                        'criteria': self._get_seq_expr(
                            'task9',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7__2',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__2',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task9__2',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10__2',
                        'criteria': self._get_seq_expr(
                            'task8',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task10__2',
                        'criteria': self._get_seq_expr(
                            'task9',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__2',
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__2',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6__2',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__3',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__3',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task9__3',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10__3',
                        'criteria': self._get_seq_expr(
                            'task8',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task10__3',
                        'criteria': self._get_seq_expr(
                            'task9',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7__4',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__4',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task9__4',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10__4',
                        'criteria': self._get_seq_expr(
                            'task8',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task10__4',
                        'criteria': self._get_seq_expr(
                            'task9',
                            'on-success'
                        )
                    }
                ]
            ],
            'multigraph': False
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        expected_task_seq = [
            'task1',
            'task2',
            'task3',
            'task4__1',
            'task4__2',
            'task5__1',
            'task6__1',
            'task5__2',
            'task6__2',
            'task7__1',
            'task7__2',
            'task7__3',
            'task7__4',
            'task8__1',
            'task9__1',
            'task8__2',
            'task9__2',
            'task8__3',
            'task9__3',
            'task8__4',
            'task9__4',
            'task10__1',
            'task10__2',
            'task10__3',
            'task10__4'
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)

    def test_splits_extra_join(self):
        wf_name = 'splits-join'

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
                    'id': 'task4',
                    'splits': ['task4']
                },
                {
                    'id': 'task5',
                    'splits': ['task4']
                },
                {
                    'id': 'task6',
                    'splits': ['task4']
                },
                {
                    'id': 'task7',
                    'join': 'all',
                    'splits': ['task4']
                },
                {
                    'id': 'task8',
                    'join': 'all',
                    'splits': ['task4']
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task8',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': False
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
                    'id': 'task4__1',
                    'name': 'task4',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task5__1',
                    'name': 'task5',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task7__1',
                    'name': 'task7',
                    'join': 'all',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task8__1',
                    'name': 'task8',
                    'join': 'all',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task6__1',
                    'name': 'task6',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task3',
                    'name': 'task3'
                },
                {
                    'id': 'task4__2',
                    'name': 'task4',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task5__2',
                    'name': 'task5',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task7__2',
                    'name': 'task7',
                    'join': 'all',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task8__2',
                    'name': 'task8',
                    'join': 'all',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task6__2',
                    'name': 'task6',
                    'splits': [('task4', 2)]
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task8__1',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task8__2',
                        'criteria': self._get_seq_expr(
                            'task1',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__1',
                        'criteria': self._get_seq_expr(
                            'task2',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__1',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6__1',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__1',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__1',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__1',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__2',
                        'criteria': self._get_seq_expr(
                            'task3',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__2',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                    {
                        'id': 'task6__2',
                        'criteria': self._get_seq_expr(
                            'task4',
                            'on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__2',
                        'criteria': self._get_seq_expr(
                            'task5',
                            'on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__2',
                        'criteria': self._get_seq_expr(
                            'task7',
                            'on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__2',
                        'criteria': self._get_seq_expr(
                            'task6',
                            'on-success'
                        )
                    }
                ]
            ],
            'multigraph': False
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        expected_task_seq = [
            'task1',
            'task2',
            'task3',
            'task4__1',
            'task4__2',
            'task5__1',
            'task6__1',
            'task5__2',
            'task6__2',
            'task7__1',
            'task7__2',
            'task8__1',
            'task8__2'
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)
