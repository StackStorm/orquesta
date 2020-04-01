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

        self.assert_spec_inspection(wf_name)

        # Mock successful join.
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

        # Both tasks before the join, task3 and task5, failed.
        # The criteria for the join task is not met.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task3',
            'task5'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task3
            statuses.FAILED       # task5
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_workflow_status=statuses.FAILED
        )

        # First task before the join, task3, failed.
        # The criteria for the join task is not met.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task3
            statuses.FAILED       # task5
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_workflow_status=statuses.FAILED
        )

        # Second task before the join, task5, failed.
        # The criteria for the join task is not met.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task3
            statuses.SUCCEEDED    # task5
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_workflow_status=statuses.FAILED
        )

    def test_join_count(self):
        wf_name = 'join-count'

        self.assert_spec_inspection(wf_name)

        # Mock one of the branches is still running and
        # the join task is satisfied and completed.
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

        # Mock error at task6 of branch 3. The criteria for join task is met.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            ('noop', 1),
            'task3',
            'task5',
            'task8'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task6
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task5
            statuses.SUCCEEDED    # task8
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task6__t0']]
        )

        # Mock error at task7 of branch 3. The criteria for join task is not met.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task3',
            'task5',
            'task7',
            ('noop', 1),
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
            statuses.SUCCEEDED    # task8
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task7__t0']]
        )

        # Mock errors at branch 2 and 3. The criteria for join task is not met.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task3',
            'task5',
            ('noop', 1),
            'task7',
            ('noop', 2)
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED,   # task3
            statuses.FAILED,      # task5
            statuses.FAILED,      # task7
        ]

        conductor = self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task5__t0'], ['task7__t0']],
            expected_workflow_status=statuses.FAILED
        )

        expected_errors = [
            {
                "message": "Execution failed. See result for details.",
                "type": "error",
                "task_id": "task5"
            },
            {
                "message": "Execution failed. See result for details.",
                "type": "error",
                "task_id": "task7"
            },
            {
                "message": (
                    "UnreachableJoinError: The join task|route \"task8|0\" "
                    "is partially satisfied but unreachable."

                ),
                "type": "error",
                "route": 0,
                "task_id": "task8"
            }
        ]

        self.assertListEqual(conductor.errors, expected_errors)

        # Mock all branches failed. The criteria for join task is not met.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task3',
            'task5',
            ('noop', 1),
            'task7',
            ('noop', 2)
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task6
            statuses.FAILED,      # task3
            statuses.FAILED,      # task5
            statuses.FAILED,      # task7
        ]

        conductor = self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task5__t0'], ['task7__t0']],
            expected_workflow_status=statuses.FAILED
        )

        expected_errors = [
            {
                "message": "Execution failed. See result for details.",
                "type": "error",
                "task_id": "task3"
            },
            {
                "message": "Execution failed. See result for details.",
                "type": "error",
                "task_id": "task5"
            },
            {
                "message": "Execution failed. See result for details.",
                "type": "error",
                "task_id": "task7"
            }
        ]

        self.assertListEqual(conductor.errors, expected_errors)

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

    def test_join_task_transition_on_completed(self):
        wf_name = 'join-on-complete'

        self.assert_spec_inspection(wf_name)

        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task3',
            'task5',
            'task6',
            'task7'
        ]

        # The tasks before the join, task3 and task5, both succeeded.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # First tasks before the join, task3, failed.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task3
            statuses.SUCCEEDED,   # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # Second tasks before the join, task5, failed.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task3
            statuses.FAILED,      # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # Both tasks before the join, task3 and task5, failed.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task3
            statuses.FAILED,      # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

    def test_join_task_transition_on_failed(self):
        wf_name = 'join-on-fail'

        self.assert_spec_inspection(wf_name)

        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task3',
            'task5'
        ]

        # The tasks before the join, task3 and task5, both succeeded.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED    # task5
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # First task before the join, task3, failed.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task3
            statuses.SUCCEEDED    # task5
        ]

        conductor = self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_workflow_status=statuses.FAILED
        )

        expected_errors = [
            {
                "message": "Execution failed. See result for details.",
                "type": "error",
                "task_id": "task3"
            },
            {
                "message": (
                    "UnreachableJoinError: The join task|route \"task6|0\" "
                    "is partially satisfied but unreachable."

                ),
                "type": "error",
                "route": 0,
                "task_id": "task6"
            }
        ]

        self.assertListEqual(conductor.errors, expected_errors)

        # Second task before the join, task5, failed.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task3
            statuses.FAILED       # task5
        ]

        conductor = self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_workflow_status=statuses.FAILED
        )

        expected_errors = [
            {
                "message": "Execution failed. See result for details.",
                "type": "error",
                "task_id": "task5"
            },
            {
                "message": (
                    "UnreachableJoinError: The join task|route \"task6|0\" "
                    "is partially satisfied but unreachable."

                ),
                "type": "error",
                "route": 0,
                "task_id": "task6"
            }
        ]

        self.assertListEqual(conductor.errors, expected_errors)

        # The tasks before the join, task3 and task5, both failed.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task3',
            'task5',
            'task6',
            'task7'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task3
            statuses.FAILED,      # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        conductor = self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

    def test_join_all_complex(self):
        # In this workflow, both tasks task3 and task5 join all to task6. There
        # are multiple task transition with different criteria (succeeded,
        # failed, and completed) defined at each task. All the task transition
        # specify task6 as the next task. Previously, join all requires all
        # inbound task transition to pass criteria. So for cases where tasks
        # defined multiple task transition with different criteria to the same
        # join task, the join all will never be satisfied. This is now changed
        # such that as long as there is at least one inbound task transition
        # from the same task that meets criteria, then the join condition from
        # that inbound task is considered satisfied.
        wf_name = 'join-all-complex'

        self.assert_spec_inspection(wf_name)

        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task3',
            'task5',
            'task6',
            'task7'
        ]

        # Both branch 1 and 2 succeeded.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # Both branch 1 and 2 failed and triggering a different
        # set of task transition but will still join on task6.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task3
            statuses.FAILED,      # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # One of the branches failed and the other succeeded. Each
        # of the task will trigger different criteria in the task
        # transition but will still join on task6.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task3
            statuses.SUCCEEDED,   # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # One of the branches succeeded and the other failed. Each
        # of the task will trigger different criteria in the task
        # transition but will still join on task6.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task3
            statuses.FAILED,      # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # Both branch 1 and 2 failed but trigger the set of task
        # transition that will pass on the failed criteria.
        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task3
            statuses.FAILED,      # task5
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

    def test_join_count_complex(self):
        # In this workflow, the task task3, task5, and task7 join on task8.
        # There are multiple task transition with different criteria (succeeded,
        # failed, and completed) defined at each task. All the task transition
        # specify task8 as the next task. Previously, join count tallies any
        # inbound task transition to pass criteria. This is now changed such
        # that the count is on the inbound task and not on task transition.
        wf_name = 'join-count-complex'

        self.assert_spec_inspection(wf_name)

        # All branches succeeded.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            'task3',
            'task5',
            'task7',
            'task8',
            'task9'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task5
            statuses.SUCCEEDED,   # task7
            statuses.SUCCEEDED,   # task8
            statuses.SUCCEEDED    # task9
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses
        )

        # One of three branches failed. Branch 3 failed.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            'task6',
            ('noop', 1),
            'task3',
            'task5',
            'task8',
            'task9'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task6
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task5
            statuses.SUCCEEDED,   # task8
            statuses.SUCCEEDED    # task9
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task6__t0']]
        )

        # One of three branches failed. Branch 2 failed.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            ('noop', 1),
            'task6',
            'task3',
            'task7',
            'task8',
            'task9'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.FAILED,      # task4
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task7
            statuses.SUCCEEDED,   # task8
            statuses.SUCCEEDED    # task9
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task4__t0']]
        )

        # One of three branches failed. Branch 1 failed.
        expected_task_seq = [
            'task1',
            'task2',
            ('noop', 1),
            'task4',
            'task6',
            'task5',
            'task7',
            'task8',
            'task9'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.FAILED,      # task2
            statuses.SUCCEEDED,   # task4
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED,   # task3
            statuses.SUCCEEDED,   # task7
            statuses.SUCCEEDED,   # task8
            statuses.SUCCEEDED    # task9
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task2__t0']]
        )

        # Two of three branches failed. Branch 2 and 3 failed.
        expected_task_seq = [
            'task1',
            'task2',
            'task4',
            ('noop', 1),
            'task6',
            ('noop', 2),
            'task3'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.SUCCEEDED,   # task2
            statuses.FAILED,      # task4
            statuses.FAILED,      # task6
            statuses.SUCCEEDED    # task3
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task4__t0'], ['task6__t0']],
            expected_workflow_status=statuses.FAILED
        )

        # Two of three branches failed. Branch 1 and 2 failed.
        expected_task_seq = [
            'task1',
            'task2',
            ('noop', 1),
            'task4',
            ('noop', 2),
            'task6',
            'task7'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.FAILED,      # task2
            statuses.FAILED,      # task4
            statuses.SUCCEEDED,   # task6
            statuses.SUCCEEDED    # task7
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task2__t0'], ['task4__t0']],
            expected_workflow_status=statuses.FAILED
        )

        # Two of three branches failed. Branch 1 and 3 failed.
        expected_task_seq = [
            'task1',
            'task2',
            ('noop', 1),
            'task4',
            'task6',
            ('noop', 2),
            'task5'
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1
            statuses.FAILED,      # task2
            statuses.SUCCEEDED,   # task4
            statuses.FAILED,      # task6
            statuses.SUCCEEDED    # task5
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=[[], ['task2__t0'], ['task6__t0']],
            expected_workflow_status=statuses.FAILED
        )
