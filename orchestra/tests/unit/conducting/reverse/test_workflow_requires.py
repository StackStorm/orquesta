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


class RequiresWorkflowConductorTest(base.ReverseWorkflowConductorTest):

    def test_multiple_requires(self):
        wf_name = 'requires'

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
