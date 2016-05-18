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

from orchestra import states
from orchestra.tests.unit import base


class JoinWorkflowConductorTest(base.WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'mistral'
        super(JoinWorkflowConductorTest, cls).setUpClass()

    @mock.patch.object(
        uuid,
        'uuid4',
        mock.MagicMock(side_effect=range(101, 200))
    )
    def test_join(self):
        wf_name = 'join'

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
                    },
                    {
                        'id': 'task6',
                        'join': True
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
                    [
                        {
                            'id': 'task6',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task3',
                                states.SUCCESS
                            )
                        }
                    ],
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
                    [
                        {
                            'id': 'task6',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task5',
                                states.SUCCESS
                            )
                        }
                    ],
                    [
                        {
                            'id': 'task7',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task6',
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
                    'name': 'task6',
                    'join': True
                },
                {
                    'id': '105',
                    'name': 'task7'
                },
                {
                    'id': '106',
                    'name': 'task4'
                },
                {
                    'id': '107',
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
                        'id': '106',
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
                [
                    {
                        'id': '104',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task3',
                            states.SUCCESS
                        )
                    }
                ],
                [
                    {
                        'id': '105',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task6',
                            states.SUCCESS
                        )
                    }
                ],
                [],
                [
                    {
                        'id': '107',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            states.SUCCESS
                        )
                    }
                ],
                [
                    {
                        'id': '104',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            states.SUCCESS
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
            '106',  # task4
            '103',  # task3
            '107',  # task5
            '104',  # task6
            '105'   # task7
        ]

        self._assert_conduct(expected_wf_ex_graph, expected_task_seq)
