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

from orquesta.tests.unit.composition.native import base


class BasicWorkflowRoutesTest(base.OrchestraWorkflowComposerTest):

    def test_sequential(self):
        wf_name = 'sequential'

        expected_routes = [
            {
                'tasks': [
                    'noop',
                    'task1',
                    'task2',
                    'task3'
                ],
                'path': [
                    ('task1', 'task2', 0),
                    ('task2', 'task3', 0),
                    ('task3', 'noop', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)

    def test_parallel(self):
        wf_name = 'parallel'

        expected_routes = [
            {
                'tasks': [
                    'task1',
                    'task2',
                    'task3'
                ],
                'path': [
                    ('task1', 'task2', 0),
                    ('task2', 'task3', 0)
                ]
            },
            {
                'tasks': [
                    'task4',
                    'task5',
                    'task6'
                ],
                'path': [
                    ('task4', 'task5', 0),
                    ('task5', 'task6', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)

    def test_branching(self):
        wf_name = 'branching'

        expected_routes = [
            {
                'tasks': [
                    'task1',
                    'task2',
                    'task3'
                ],
                'path': [
                    ('task1', 'task2', 0),
                    ('task2', 'task3', 0)
                ]
            },
            {
                'tasks': [
                    'task1',
                    'task4',
                    'task5'
                ],
                'path': [
                    ('task1', 'task4', 0),
                    ('task4', 'task5', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)

    def test_decision(self):
        wf_name = 'decision'

        expected_routes = [
            {
                'tasks': [
                    'a',
                    't1'
                ],
                'path': [
                    ('t1', 'a', 0),
                ]
            },
            {
                'tasks': [
                    'b',
                    't1'
                ],
                'path': [
                    ('t1', 'b', 0),
                ]
            },
            {
                'tasks': [
                    'c',
                    't1'
                ],
                'path': [
                    ('t1', 'c', 0),
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)
