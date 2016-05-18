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
from orchestra import states
from orchestra.tests.unit import base
from orchestra.utils import plugin


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
                                states.SUCCESS
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task3',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task2',
                                states.SUCCESS
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
                    'name': 'task1'
                },
                {
                    'id': '102',
                    'name': 'task2'
                },
                {
                    'id': '103',
                    'name': 'task3'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': '102',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            states.SUCCESS
                        )
                    }
                ],
                [
                    {
                        'id': '103',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task2',
                            states.SUCCESS
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
                                states.SUCCESS
                            )
                        },
                        {
                            'id': 'task4',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task1',
                                states.SUCCESS
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task3',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task2',
                                states.SUCCESS
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
                                states.SUCCESS
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
                    'name': 'task1'
                },
                {
                    'id': '102',
                    'name': 'task2'
                },
                {
                    'id': '103',
                    'name': 'task3'
                },
                {
                    'id': '104',
                    'name': 'task4'
                },
                {
                    'id': '105',
                    'name': 'task5'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': '102',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            states.SUCCESS
                        )
                    },
                    {
                        'id': '104',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task1',
                            states.SUCCESS
                        )
                    }
                ],
                [
                    {
                        'id': '103',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task2',
                            states.SUCCESS
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
                            states.SUCCESS
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
                                states.SUCCESS,
                                "$.which = 'a'"
                            )
                        },
                        {
                            'id': 'b',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                't1',
                                states.SUCCESS,
                                "$.which = 'b'"
                            )
                        },
                        {
                            'id': 'c',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                't1',
                                states.SUCCESS,
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
                    'name': 't1'
                },
                {
                    'id': '102',
                    'name': 'a'
                },
                {
                    'id': '103',
                    'name': 'b'
                },
                {
                    'id': '104',
                    'name': 'c'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': '102',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            't1',
                            states.SUCCESS,
                            "$.which = 'a'"
                        )
                    },
                    {
                        'id': '103',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            't1',
                            states.SUCCESS,
                            "$.which = 'b'"
                        )
                    },
                    {
                        'id': '104',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            't1',
                            states.SUCCESS,
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
