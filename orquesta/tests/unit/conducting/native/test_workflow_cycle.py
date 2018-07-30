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


class CyclicWorkflowConductorTest(base.OrchestraWorkflowConductorTest):

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

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq)

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

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq)

    def test_rollback_retry(self):
        wf_name = 'rollback-retry'

        expected_task_seq = [
            'init',
            'check',
            'create',
            'rollback',
            'check',
            'delete'
        ]

        mock_states = [
            states.SUCCEEDED,   # init
            states.FAILED,      # check
            states.SUCCEEDED,   # create
            states.SUCCEEDED,   # rollback
            states.SUCCEEDED,   # check
            states.SUCCEEDED    # delete
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq, mock_states=mock_states)
