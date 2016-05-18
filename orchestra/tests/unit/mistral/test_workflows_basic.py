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

from orchestra.composers import openstack
from orchestra.utils import plugin
from orchestra.tests.unit import base


class BasicWorkflowConductorTest(base.WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'mistral'
        super(BasicWorkflowConductorTest, cls).setUpClass()

    @mock.patch.object(
        uuid,
        'uuid4',
        mock.MagicMock(side_effect=range(101, 200))
    )
    def test_sequential(self):
        wf_name = 'sequential'

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
                        }
                    ],
                    [
                        {
                            'id': 'task3',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task2',
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
                    'workflow': wf_name
                },
                {
                    'id': '102',
                    'name': 'task2',
                    'workflow': wf_name
                },
                {
                    'id': '103',
                    'name': 'task3',
                    'workflow': wf_name
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
                []
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

        expected_task_seq = [
            '101',  # task1
            '102',  # task2
            '103'   # task3
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)

    @mock.patch.object(
        uuid,
        'uuid4',
        mock.MagicMock(side_effect=range(101, 200))
    )
    def test_branching(self):
        wf_name = 'branching'

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
                        'id': 'task4'
                    },
                    {
                        'id': 'task5'
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
                            'id': 'task4',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task1',
                                'succeeded'
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task3',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task2',
                                'succeeded'
                            )
                        }
                    ],
                    [],
                    [
                        {
                            'id': 'task5',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task4',
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
                    'workflow': wf_name
                },
                {
                    'id': '102',
                    'name': 'task2',
                    'workflow': wf_name
                },
                {
                    'id': '103',
                    'name': 'task3',
                    'workflow': wf_name
                },
                {
                    'id': '104',
                    'name': 'task4',
                    'workflow': wf_name
                },
                {
                    'id': '105',
                    'name': 'task5',
                    'workflow': wf_name
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
                        'id': '104',
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
                [],
                [
                    {
                        'id': '105',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
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
            '104',  # task4
            '103',  # task3
            '105'   # task5
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)

    @mock.patch.object(
        uuid,
        'uuid4',
        mock.MagicMock(side_effect=range(101, 200))
    )
    def test_decision_tree(self):
        wf_name = 'decision'

        expected_wf_graphs = {
            wf_name: {
                'directed': True,
                'graph': {},
                'nodes': [
                    {
                        'id': 't1'
                    },
                    {
                        'id': 'a'
                    },
                    {
                        'id': 'b'
                    },
                    {
                        'id': 'c'
                    }
                ],
                'adjacency': [
                    [
                        {
                            'id': 'a',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                't1',
                                'succeeded',
                                "$.which = 'a'"
                            )
                        },
                        {
                            'id': 'b',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                't1',
                                'succeeded',
                                "$.which = 'b'"
                            )
                        },
                        {
                            'id': 'c',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                't1',
                                'succeeded',
                                "not $.which in list(a, b)"
                            )
                        }
                    ],
                    [],
                    [],
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
                    'name': 't1',
                    'workflow': wf_name
                },
                {
                    'id': '102',
                    'name': 'a',
                    'workflow': wf_name
                },
                {
                    'id': '103',
                    'name': 'b',
                    'workflow': wf_name
                },
                {
                    'id': '104',
                    'name': 'c',
                    'workflow': wf_name
                }
            ],
            'adjacency': [
                [
                    {
                        'id': '102',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            't1',
                            'succeeded',
                            "$.which = 'a'"
                        )
                    },
                    {
                        'id': '103',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            't1',
                            'succeeded',
                            "$.which = 'b'"
                        )
                    },
                    {
                        'id': '104',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            't1',
                            'succeeded',
                            "not $.which in list(a, b)"
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

        # Test branch "a"
        expected_task_seq = [
            '101',  # t1
            '102'   # a
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            which='a'
        )

        # Test branch "b"
        expected_task_seq = [
            '101',  # t1
            '103'   # b
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            which='b'
        )

        # Test branch "c"
        expected_task_seq = [
            '101',  # t1
            '104'   # c
        ]

        self._assert_conduct(
            expected_wf_ex_graph,
            expected_task_seq,
            which='c'
        )
