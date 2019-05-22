# Copyright 2019 Extreme Networks, Inc.
#
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

from orquesta import statuses
from orquesta.tests.unit.conducting.native import base


class JoinWorkflowConductorTest(base.OrchestraWorkflowConductorTest):

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

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq)

    def test_join_count(self):
        wf_name = 'join-count'

        self.assert_spec_inspection(wf_name)

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

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.RUNNING,     # task6
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task5
            statuses.SUCCEEDED    # task8
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_workflow_status=statuses.RUNNING
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

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task5
            statuses.RUNNING,     # task7
            statuses.SUCCEEDED    # task8
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_workflow_status=statuses.RUNNING
        )

    def test_join_count_with_branch_error(self):
        wf_name = 'join-count'

        self.assert_spec_inspection(wf_name)

        # Mock error at task6. The conductor runs breadth first
        # and so task3 and task5 has not started.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED       # task6
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_workflow_status=statuses.FAILED
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
            'noop',
            'task8'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task5
            statuses.FAILED,      # task7
            statuses.SUCCEEDED,   # noop
            statuses.SUCCEEDED    # task8
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

    def test_join_context(self):
        wf_name = 'join-context'

        expected_output = {
            'messages': [
                'Fee fi fo fum',
                'I smell the blood of an English man',
                'Be alive, or be he dead',
                "I'll grind his bones to make my bread"
            ]
        }

        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task8',
            'task3',
            'task5',
            'task7',
            'task9',
            'task10'
        ]

        mock_results = [
            None,                                   # task1
            'Fee fi',                               # task2
            'I smell the blood of an English man',  # task4
            None,                                   # task6
            None,                                   # task8
            'fo fum',                               # task3
            None,                                   # task5
            'Be alive, or be he dead',              # task7
            None,                                   # task9
            None                                    # task10
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_results=mock_results,
            expected_output=expected_output
        )
