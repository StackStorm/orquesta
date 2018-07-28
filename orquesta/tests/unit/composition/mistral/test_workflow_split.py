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

from orquesta.tests.unit.composition.mistral import base


class SplitWorkflowComposerTest(base.MistralWorkflowComposerTest):

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
                    'barrier': '*',
                    'splits': ['task4']
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6',
                            condition='on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

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
                    'barrier': '*',
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
                    'barrier': '*',
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
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__1',
                            condition='on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5__1',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6__1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__2',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__2',
                            condition='on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5__2',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6__2',
                            condition='on-success'
                        )
                    }
                ]
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

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
                    'barrier': '*',
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
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7',
                            condition='on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

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
                    'barrier': '*',
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
                    'barrier': '*',
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
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task8__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__1',
                            condition='on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5__1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__1',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6__1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__2',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__2',
                            condition='on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5__2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__2',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6__2',
                            condition='on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

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
                    'barrier': '*',
                    'splits': ['task4', 'task7']
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task9',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task8',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task9',
                            condition='on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

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
                    'barrier': '*',
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
                    'barrier': '*',
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
                    'barrier': '*',
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
                    'barrier': '*',
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
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__1',
                            condition='on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5__1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task9__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task8__1',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task10__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task9__1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6__1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__2',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task9__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task8__2',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task10__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task9__2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__2',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__2',
                            condition='on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5__2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__3',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task9__3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10__3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task8__3',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task10__3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task9__3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7__4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6__2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__4',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task9__4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__4',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task10__4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task8__4',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task10__4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task9__4',
                            condition='on-success'
                        )
                    }
                ]
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

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
                    'barrier': '*',
                    'splits': ['task4']
                },
                {
                    'id': 'task8',
                    'barrier': '*',
                    'splits': ['task4']
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7',
                            condition='on-success'
                        )
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

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
                    'barrier': '*',
                    'splits': [('task4', 1)]
                },
                {
                    'id': 'task8__1',
                    'name': 'task8',
                    'barrier': '*',
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
                    'barrier': '*',
                    'splits': [('task4', 2)]
                },
                {
                    'id': 'task8__2',
                    'name': 'task8',
                    'barrier': '*',
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
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task8__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task8__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__1',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__1',
                            condition='on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5__1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__1',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__1',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6__1',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task4__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task3',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task5__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__2',
                            condition='on-success'
                        )
                    },
                    {
                        'id': 'task6__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task4__2',
                            condition='on-success'
                        )
                    },
                ],
                [
                    {
                        'id': 'task7__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task5__2',
                            condition='on-success'
                        )
                    }
                ],
                [
                    {
                        'id': 'task8__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task7__2',
                            condition='on-success'
                        )
                    }
                ],
                [],
                [
                    {
                        'id': 'task7__2',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            'task6__2',
                            condition='on-success'
                        )
                    }
                ]
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)
