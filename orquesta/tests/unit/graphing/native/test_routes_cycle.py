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


class CyclicWorkflowRoutesTest(base.OrchestraWorkflowComposerTest):

    def test_cycle(self):
        wf_name = 'cycle'

        expected_routes = [
            {
                'tasks': [
                    'prep',
                    'task1',
                    'task2',
                    'task3'
                ],
                'path': [
                    ('prep', 'task1', 0),
                    ('task1', 'task2', 0),
                    ('task2', 'task3', 0),
                    ('task3', 'task1', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)

    def test_cycles(self):
        wf_name = 'cycles'

        expected_routes = [
            {
                'tasks': [
                    'prep',
                    'task1',
                    'task2',
                    'task3',
                    'task4',
                    'task5'
                ],
                'path': [
                    ('prep', 'task1', 0),
                    ('task1', 'task2', 0),
                    ('task2', 'task3', 0),
                    ('task2', 'task5', 0),
                    ('task3', 'task4', 0),
                    ('task4', 'task2', 0),
                    ('task5', 'task1', 0)
                ]
            }
        ]

        self.assert_wf_ex_routes(wf_name, expected_routes)
