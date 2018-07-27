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

from orquesta import states
from orquesta.tests.unit.conducting.mistral import base


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
            states.SUCCEEDED,   # task1
            states.SUCCEEDED,   # task2
            states.SUCCEEDED,   # task4
            states.RUNNING,     # task6
            states.SUCCEEDED,   # task3
            states.SUCCEEDED,   # task5
            states.SUCCEEDED    # task8
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
            states.SUCCEEDED,   # task1
            states.SUCCEEDED,   # task2
            states.SUCCEEDED,   # task4
            states.SUCCEEDED,   # task6
            states.SUCCEEDED,   # task3
            states.SUCCEEDED,   # task5
            states.RUNNING,     # task7
            states.SUCCEEDED    # task8
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )

    def test_join_count_with_branch_error(self):
        wf_name = 'join-count'

        # Mock error at task6, note that task3 and task5 are
        # already in running state when task6 failed.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task3',
            'task5'
        ]

        mock_states = [
            states.SUCCEEDED,   # task1
            states.SUCCEEDED,   # task2
            states.SUCCEEDED,   # task4
            states.FAILED,      # task6
            states.SUCCEEDED,   # task3
            states.SUCCEEDED    # task5
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )

        # Mock error at task7, note that task3 and task5 have
        # already satisfied the task8 join requirements.
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
            states.SUCCEEDED,   # task1
            states.SUCCEEDED,   # task2
            states.SUCCEEDED,   # task4
            states.SUCCEEDED,   # task6
            states.SUCCEEDED,   # task3
            states.SUCCEEDED,   # task5
            states.FAILED,      # task7
            states.SUCCEEDED    # task8
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )
