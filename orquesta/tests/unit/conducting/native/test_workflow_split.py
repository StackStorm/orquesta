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


class SplitWorkflowConductorTest(base.OrchestraWorkflowConductorTest):

    def test_split(self):
        wf_name = 'split'

        expected_task_seq = [
            'task1',
            'task2',
            'task3',
            'task4__1',
            'task4__2',
            'task5__1',
            'task6__1',
            'task5__2',
            'task6__2',
            'task7__1',
            'task7__2'
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq)

    def test_splits(self):
        wf_name = 'splits'

        expected_task_seq = [
            'task1',
            'task2',
            'task3',
            'task8__1',
            'task4__1',
            'task4__2',
            'task5__1',
            'task6__1',
            'task5__2',
            'task6__2',
            'task7__1',
            'task7__2',
            'task8__2',
            'task8__3'
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq)

    def test_nested_splits(self):
        wf_name = 'splits-nested'

        expected_task_seq = [
            'task1',
            'task2',
            'task3',
            'task4__1',
            'task4__2',
            'task5__1',
            'task6__1',
            'task5__2',
            'task6__2',
            'task7__1',
            'task7__2',
            'task7__3',
            'task7__4',
            'task8__1',
            'task9__1',
            'task8__2',
            'task9__2',
            'task8__3',
            'task9__3',
            'task8__4',
            'task9__4',
            'task10__1',
            'task10__2',
            'task10__3',
            'task10__4'
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq)

    def test_splits_extra_join(self):
        wf_name = 'splits-join'

        expected_task_seq = [
            'task1',
            'task2',
            'task3',
            'task4__1',
            'task4__2',
            'task5__1',
            'task6__1',
            'task5__2',
            'task6__2',
            'task7__1',
            'task7__2',
            'task8__1',
            'task8__2'
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq)

    def test_splits_mixed(self):
        wf_name = 'splits-mixed'

        expected_task_seq = [
            'task1',
            'task2',
            'task3__1',
            'task3__2',
            'task4__1',
            'task4__3',
            'task5__1',
            'task5__3'
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq
        )

    def test_splits_mixed_alt_branch(self):
        wf_name = 'splits-mixed'

        expected_task_seq = [
            'task1',
            'task2',
            'task3__1',
            'task3__2',
            'task4__1',
            'task4__4',
            'task5__1',
            'task5__4'
        ]

        mock_states = [
            states.SUCCEEDED,   # task1
            states.SUCCEEDED,   # task2
            states.SUCCEEDED,   # task3__1
            states.FAILED,      # task3__2
            states.SUCCEEDED,   # task4__1
            states.SUCCEEDED,   # task4__4
            states.SUCCEEDED,   # task5__1
            states.SUCCEEDED    # task5__4
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states
        )
