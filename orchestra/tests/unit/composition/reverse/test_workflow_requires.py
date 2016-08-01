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

from orchestra.tests.unit import base


class RequiresWorkflowComposerTest(base.ReverseWorkflowComposerTest):

    def test_multiple_requires(self):
        wf_name = 'requires'

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
                        'criteria': self.compose_seq_expr('task1')
                    },
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task1')
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task2')
                    }
                ],
                [
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task3')
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task4')
                    }
                ],
                [
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task5')
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task6')
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
                        'criteria': self.compose_seq_expr('task1')
                    },
                    {
                        'id': 'task4',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task1')
                    }
                ],
                [
                    {
                        'id': 'task3',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task2')
                    }
                ],
                [
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task3')
                    }
                ],
                [
                    {
                        'id': 'task5',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task4')
                    }
                ],
                [
                    {
                        'id': 'task6',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task5')
                    }
                ],
                [
                    {
                        'id': 'task7',
                        'key': 0,
                        'criteria': self.compose_seq_expr('task6')
                    }
                ],
                []
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)
