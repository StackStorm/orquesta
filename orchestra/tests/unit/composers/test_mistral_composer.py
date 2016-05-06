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
from orchestra.tests.unit.composers import base


class MistralWorkflowComposerTest(base.WorkflowComposerTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'mistral'
        super(MistralWorkflowComposerTest, cls).setUpClass()

    def test_get_composer(self):
        self.assertEqual(
            plugin.get_module('orchestra.composers', self.composer_name),
            openstack.MistralWorkflowComposer
        )

    def test_exception_empty_definition(self):
        self.assertRaises(
            ValueError,
            self.composer.compose,
            {}
        )

    def test_exception_unidentified_entry(self):
        self.assertRaises(
            KeyError,
            self.composer.compose,
            self._get_wf_def('sequential'),
            entry='foobar'
        )

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
                    [
                        {
                            'id': 'task6',
                            'key': 0,
                            'criteria': self._get_seq_expr(
                                'task3',
                                'succeeded'
                            )
                        }
                    ],
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
                    [
                        {
                            'id': 'task6',
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
                    'workflow': 'join'
                },
                {
                    'id': '102',
                    'name': 'task2',
                    'workflow': 'join'
                },
                {
                    'id': '103',
                    'name': 'task3',
                    'workflow': 'join'
                },
                {
                    'id': '104',
                    'name': 'task6',
                    'workflow': 'join',
                    'join': True
                },
                {
                    'id': '105',
                    'name': 'task7',
                    'workflow': 'join'
                },
                {
                    'id': '106',
                    'name': 'task4',
                    'workflow': 'join',
                },
                {
                    'id': '107',
                    'name': 'task5',
                    'workflow': 'join'
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
                        'id': '106',
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
                            'task3',
                            'succeeded'
                        )
                    }
                ],
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
                [],
                [
                    {
                        'id': '107',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task4',
                            'succeeded'
                        )
                    }
                ],
                [
                    {
                        'id': '104',
                        'key': 0,
                        'criteria': self._get_seq_expr(
                            'task5',
                            'succeeded'
                        )
                    }
                ]
            ],
            'multigraph': True
        }

        self._assert_compose(wf_name, expected_wf_ex_graph)

    @mock.patch.object(
        uuid,
        'uuid4',
        mock.MagicMock(side_effect=range(101, 200))
    )
    def test_merge(self):
        wf_name = 'merge'

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
                    'workflow': 'merge'
                },
                {
                    'id': '102',
                    'name': 'task2',
                    'workflow': 'merge'
                },
                {
                    'id': '103',
                    'name': 'task4',
                    'workflow': 'merge'
                },
                {
                    'id': '104',
                    'name': 'task5',
                    'workflow': 'merge'
                },
                {
                    'id': '105',
                    'name': 'task7',
                    'workflow': 'merge',
                    'join': True
                },
                {
                    'id': '106',
                    'name': 'task6',
                    'workflow': 'merge'
                },
                {
                    'id': '107',
                    'name': 'task3',
                    'workflow': 'merge'
                },
                {
                    'id': '108',
                    'name': 'task4',
                    'workflow': 'merge'
                },
                {
                    'id': '109',
                    'name': 'task5',
                    'workflow': 'merge'
                },
                {
                    'id': '110',
                    'name': 'task7',
                    'workflow': 'merge',
                    'join': True
                },
                {
                    'id': '111',
                    'name': 'task6',
                    'workflow': 'merge'
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
