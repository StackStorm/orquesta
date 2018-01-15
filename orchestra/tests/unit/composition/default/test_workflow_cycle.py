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

from orchestra.tests.unit.composition.default import base


class CyclicWorkflowComposerTest(base.OrchestraWorkflowComposerTest):

    def test_cycle(self):
        wf_name = 'cycle'

        expected_wf_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 'prep'
                },
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
                        'id': 'task1',
                        'key': 0,
                        'criteria': ['<% task_state(prep) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': ['<% task_state(task1) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': ['<% task_state(task2) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task1',
                        'key': 0,
                        'criteria': [
                            '<% task_state(task3) = "SUCCESS" and '
                            '$.count < 3 %>'
                        ]
                    }
                ]
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 'prep',
                    'name': 'prep'
                },
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
                        'id': 'task1',
                        'key': 0,
                        'criteria': ['<% task_state(prep) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': ['<% task_state(task1) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': ['<% task_state(task2) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task1',
                        'key': 0,
                        'criteria': [
                            '<% task_state(task3) = "SUCCESS" and '
                            '$.count < 3 %>'
                        ]
                    }
                ]
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_cycles(self):
        wf_name = 'cycles'

        expected_wf_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 'prep'
                },
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
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task1',
                        'key': 0,
                        'criteria': ['<% task_state(prep) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': ['<% task_state(task1) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': [
                            '<% task_state(task2) = "SUCCESS" and '
                            'not $.proceed %>'
                        ]
                    },
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': [
                            '<% task_state(task2) = "SUCCESS" and '
                            '$.proceed %>'
                        ]
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': ['<% task_state(task3) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': ['<% task_state(task4) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task1',
                        'key': 0,
                        'criteria': [
                            '<% task_state(task5) = "SUCCESS" and '
                            '$.count < 3 %>'
                        ]
                    }
                ]
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 'prep',
                    'name': 'prep'
                },
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
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'task1',
                        'key': 0,
                        'criteria': ['<% task_state(prep) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': ['<% task_state(task1) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': [
                            '<% task_state(task2) = "SUCCESS" and '
                            'not $.proceed %>'
                        ]
                    },
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': [
                            '<% task_state(task2) = "SUCCESS" and '
                            '$.proceed %>'
                        ]
                    }
                ],
                [
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': ['<% task_state(task3) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task2',
                        'key': 0,
                        'criteria': ['<% task_state(task4) = "SUCCESS" %>']
                    }
                ],
                [
                    {
                        'id': 'task1',
                        'key': 0,
                        'criteria': [
                            '<% task_state(task5) = "SUCCESS" and '
                            '$.count < 3 %>'
                        ]
                    }
                ]
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)
