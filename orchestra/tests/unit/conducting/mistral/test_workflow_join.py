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

from orchestra import states
from orchestra.tests.unit.conducting.mistral import base


class JoinWorkflowConductorTest(base.MistralWorkflowConductorTest):

    def test_join(self):
        wf_name = 'join'

        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task3',
            'task5',
            'task6',
            'task7'
        ]

        self.assert_conducting_sequences(wf_name, expected_task_seq)

    def test_join_count(self):
        wf_name = 'join-count'

        # Mock error at task6
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task3',
            'task5',
            'task8'
        ]

        mock_states = [
            states.SUCCESS,     # task1
            states.SUCCESS,     # task2
            states.SUCCESS,     # task4
            states.ERROR,       # task6
            states.SUCCESS,     # task3
            states.SUCCESS,     # task5
            states.SUCCESS      # task8
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )

        # Mock error at task7
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task3',
            'task5',
            'task7',
            'task8'
        ]

        mock_states = [
            states.SUCCESS,     # task1
            states.SUCCESS,     # task2
            states.SUCCESS,     # task4
            states.SUCCESS,     # task6
            states.SUCCESS,     # task3
            states.SUCCESS,     # task5
            states.ERROR,       # task7
            states.SUCCESS      # task8
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )
