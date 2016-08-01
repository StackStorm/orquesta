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


class CyclicWorkflowConductorTest(base.DirectWorkflowConductorTest):

    def test_cycle(self):
        wf_name = 'cycle'

        expected_task_seq = [
            'prep',
            'task1',
            'task2',
            'task3',
            'task1',
            'task2',
            'task3',
            'task1',
            'task2',
            'task3'
        ]

        mock_contexts = [
            {'count': 0},   # prep
            {'count': 0},   # task1
            {'count': 0},   # task2
            {'count': 1},   # task3
            {'count': 1},   # task1
            {'count': 1},   # task2
            {'count': 2},   # task3
            {'count': 2},   # task1
            {'count': 2},   # task2
            {'count': 3}    # task3
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_contexts=mock_contexts
        )

    def test_cycles(self):
        wf_name = 'cycles'

        expected_task_seq = [
            'prep',
            'task1',
            'task2',
            'task3',
            'task4',
            'task2',
            'task5',
            'task1',
            'task2',
            'task3',
            'task4',
            'task2',
            'task5',
            'task1',
            'task2',
            'task3',
            'task4',
            'task2',
            'task5'
        ]

        mock_contexts = [
            {'count': 0},                       # prep
            {'count': 0, 'proceed': False},     # task1
            {'count': 0, 'proceed': False},     # task2
            {'count': 0, 'proceed': False},     # task3
            {'count': 0, 'proceed': True},      # task4
            {'count': 0, 'proceed': True},      # task2
            {'count': 1, 'proceed': True},      # task5
            {'count': 1, 'proceed': False},     # task1
            {'count': 1, 'proceed': False},     # task2
            {'count': 1, 'proceed': False},     # task3
            {'count': 1, 'proceed': True},      # task4
            {'count': 1, 'proceed': True},      # task2
            {'count': 2, 'proceed': True},      # task5
            {'count': 2, 'proceed': False},     # task1
            {'count': 2, 'proceed': False},     # task2
            {'count': 2, 'proceed': False},     # task3
            {'count': 2, 'proceed': True},      # task4
            {'count': 2, 'proceed': True},      # task2
            {'count': 3, 'proceed': True}       # task5
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_contexts=mock_contexts
        )
