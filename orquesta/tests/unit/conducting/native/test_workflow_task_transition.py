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
from orquesta.tests.unit.conducting.native import base


class TaskTransitionWorkflowConductorTest(base.OrchestraWorkflowConductorTest):

    def test_on_error(self):
        wf_name = 'error-handling'

        self.assert_spec_inspection(wf_name)

        # Mock task1 success
        expected_task_seq = [
            'task1',
            'task2'
        ]

        mock_states = [
            states.SUCCEEDED,   # task1
            states.SUCCEEDED    # task2
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )

        # Mock task1 error
        expected_task_seq = [
            'task1',
            'task3'
        ]

        mock_states = [
            states.FAILED,      # task1
            states.SUCCEEDED    # task3
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )

    def test_on_complete(self):
        wf_name = 'task-on-complete'

        self.assert_spec_inspection(wf_name)

        # Mock task1 success
        expected_task_seq = [
            'task1',
            'task2',
            'task4'
        ]

        mock_states = [
            states.SUCCEEDED,   # task1
            states.SUCCEEDED,   # task2
            states.SUCCEEDED    # task4
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )

        # Mock task1 error
        expected_task_seq = [
            'task1',
            'task3',
            'task4'
        ]

        mock_states = [
            states.FAILED,      # task1
            states.SUCCEEDED,   # task3
            states.SUCCEEDED    # task4
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )

    def test_task_transitions_split(self):
        wf_name = 'task-transitions-split'

        self.assert_spec_inspection(wf_name)

        # Mock task1 success
        expected_task_seq = [
            'task1',
            'task2__1',
            'task2__3'
        ]

        mock_states = [
            states.SUCCEEDED,   # task1
            states.SUCCEEDED,   # task2__1 on-complete
            states.SUCCEEDED    # task2__3 on-success
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )

        # Mock task1 error
        expected_task_seq = [
            'task1',
            'task2__1',
            'task2__2'
        ]

        mock_states = [
            states.FAILED,      # task1
            states.SUCCEEDED,   # task2__1 on-complete
            states.SUCCEEDED    # task2__2 on-error
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )
