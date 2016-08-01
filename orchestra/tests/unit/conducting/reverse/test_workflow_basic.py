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


class BasicWorkflowConductorTest(base.ReverseWorkflowConductorTest):

    def test_sequential(self):
        wf_name = 'sequential'

        expected_task_seq = [
            'task1',
            'task2',
            'task3'
        ]

        self.assert_conducting_sequences(wf_name, expected_task_seq)

    def test_parallel(self):
        wf_name = 'parallel'

        expected_task_seq = [
            'task1',
            'task4',
            'task2',
            'task5',
            'task3',
            'task6'
        ]

        self.assert_conducting_sequences(wf_name, expected_task_seq)

    def test_branching(self):
        wf_name = 'branching'

        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task3',
            'task5'
        ]

        self.assert_conducting_sequences(wf_name, expected_task_seq)
