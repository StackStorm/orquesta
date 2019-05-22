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
from orquesta.tests.unit.conducting.mistral import base


class TaskTransitionWorkflowConductorTest(base.MistralWorkflowConductorTest):

    def test_on_error(self):
        wf_name = 'task-on-error'

        # Mock task1 success
        expected_task_seq = [
            'task1',
            'task2'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED    # task2
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # Mock task1 error
        expected_task_seq = [
            'task1',
            'task3'
        ]

        mock_statuses = [
            statuses.FAILED,      # task1
            statuses.SUCCEEDED    # task3
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

    def test_on_complete(self):
        wf_name = 'task-on-complete'

        # Mock task1 success
        expected_task_seq = [
            'task1',
            'task2',
            'task4'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED    # task4
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # Mock task1 error
        expected_task_seq = [
            'task1',
            'task3',
            'task4'
        ]

        mock_statuses = [
            statuses.FAILED,      # task1
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED    # task4
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

    def test_task_transitions_split(self):
        wf_name = 'task-transitions-split'

        # Mock task1 success
        expected_routes = [
            [],                 # default from start
            ['task1__t0'],      # task1 -> task2 (when #1)
            ['task1__t2']       # task1 -> task2 (when #3)
        ]

        expected_task_seq = [
            ('task1', 0),
            ('task2', 1),
            ('task2', 2)
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2__1 on-complete
            statuses.SUCCEEDED    # task2__3 on-success
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            expected_routes=expected_routes,
            mock_statuses=mock_statuses
        )

        # Mock task1 error
        expected_routes = [
            [],                 # default from start
            ['task1__t0'],      # task1 -> task2 (when #1)
            ['task1__t1']       # task1 -> task2 (when #2)
        ]

        expected_task_seq = [
            ('task1', 0),
            ('task2', 1),
            ('task2', 2)
        ]

        mock_statuses = [
            statuses.FAILED,      # task1
            statuses.SUCCEEDED,   # task2__1 on-complete
            statuses.SUCCEEDED    # task2__2 on-error
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            expected_routes=expected_routes,
            mock_statuses=mock_statuses
        )
