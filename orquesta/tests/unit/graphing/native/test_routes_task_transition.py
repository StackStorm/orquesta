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


class TaskTransitionWorkflowRoutesTest(base.OrchestraWorkflowComposerTest):

    def test_error_handling(self):
        wf_name = 'error-handling'

        expected_routes = [
            {
                'tasks': [
                    'task1',
                    'task2'
                ],
                'path': [
                    ('task1', 'task2', 0)
                ]
            },
            {
                'tasks': [
                    'task1',
                    'task3'
                ],
                'path': [
                    ('task1', 'task3', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)

    def test_task_with_duplicate_when(self):
        wf_name = 'task-duplicate-when'

        expected_routes = [
            {
                'tasks': [
                    'task1',
                    'task2'
                ],
                'path': [
                    ('task1', 'task2', 0)
                ]
            },
            {
                'tasks': [
                    'task1',
                    'task3'
                ],
                'path': [
                    ('task1', 'task3', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)

    def test_task_with_duplicate_transition(self):
        wf_name = 'task-duplicate-transition'

        expected_routes = [
            {
                'tasks': [
                    'task1',
                    'task2__1'
                ],
                'path': [
                    ('task1', 'task2__1', 0)
                ]
            },
            {
                'tasks': [
                    'task1',
                    'task2__2'
                ],
                'path': [
                    ('task1', 'task2__2', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)

    def test_on_complete(self):
        wf_name = 'task-on-complete'

        expected_routes = [
            {
                'tasks': [
                    'task1',
                    'task2'
                ],
                'path': [
                    ('task1', 'task2', 0)
                ]
            },
            {
                'tasks': [
                    'task1',
                    'task3'
                ],
                'path': [
                    ('task1', 'task3', 0)
                ]
            },
            {
                'tasks': [
                    'task1',
                    'task4'
                ],
                'path': [
                    ('task1', 'task4', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)

    def test_task_transitions_split(self):
        wf_name = 'task-transitions-split'

        expected_routes = [
            {
                'tasks': [
                    'task1',
                    'task2__1'
                ],
                'path': [
                    ('task1', 'task2__1', 0)
                ]
            },
            {
                'tasks': [
                    'task1',
                    'task2__2'
                ],
                'path': [
                    ('task1', 'task2__2', 0)
                ]
            },
            {
                'tasks': [
                    'task1',
                    'task2__3'
                ],
                'path': [
                    ('task1', 'task2__3', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)
