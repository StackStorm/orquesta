# Copyright 2021 The StackStorm Authors.
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

from orquesta import rehearsing
from orquesta import statuses
from orquesta.tests.unit.conducting.native import base


class JoinWorkflowConductorTest(base.OrchestraWorkflowConductorTest):
    def test_join(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_no_inbound(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join"),
            # Both tasks before the join, task3 and task5, failed.
            # The criteria for the join task is not met.
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5"],
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
                {"task_id": "task5", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_single_inbound_from_left(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join"),
            # First task before the join, task3, failed.
            # The criteria for the join task is not met.
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5"],
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_single_inbound_from_right(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join"),
            # First task before the join, task5, failed.
            # The criteria for the join task is not met.
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5"],
            "mock_action_executions": [
                {"task_id": "task5", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_satisfied_with_branch_running(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count"),
            # Mock one of the branches is still running and
            # the join task is satisfied and completed.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task6",
                "task3",
                "task5",
                "task8",
            ],
            "mock_action_executions": [
                {"task_id": "task6", "status": statuses.RUNNING},
            ],
            "expected_workflow_status": statuses.RUNNING,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_satisfied_with_branch_error_1(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count"),
            # Mock error at task6 of branch 3. The criteria for join task is met.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task6",
                "noop__r1",
                "task3",
                "task5",
                "task8",
            ],
            "expected_routes": [[], ["task6__t0"]],
            "mock_action_executions": [
                {"task_id": "task6", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_satisfied_with_branch_error_2(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count"),
            # Mock error at task7 of branch 3. The criteria for join task is met.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task6",
                "task3",
                "task5",
                "task7",
                "noop__r1",
                "task8",
            ],
            "expected_routes": [[], ["task7__t0"]],
            "mock_action_executions": [
                {"task_id": "task7", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_not_satisfied(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count"),
            # Mock errors at branch 2 and 3. The criteria for join task is not met.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task6",
                "task3",
                "task5",
                "noop__r1",
                "task7",
                "noop__r2",
            ],
            "expected_routes": [[], ["task5__t0"], ["task7__t0"]],
            "mock_action_executions": [
                {"task_id": "task5", "status": statuses.FAILED},
                {"task_id": "task7", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
            "expected_errors": [
                {
                    "message": "Execution failed. See result for details.",
                    "type": "error",
                    "task_id": "task5",
                },
                {
                    "message": "Execution failed. See result for details.",
                    "type": "error",
                    "task_id": "task7",
                },
                {
                    "message": (
                        'UnreachableJoinError: The join task|route "task8|0" '
                        "is partially satisfied but unreachable."
                    ),
                    "type": "error",
                    "route": 0,
                    "task_id": "task8",
                },
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_not_satisfied_no_inbound(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count"),
            # Mock all branches failed. The criteria for join task is not met.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task6",
                "task3",
                "task5",
                "noop__r1",
                "task7",
                "noop__r2",
            ],
            "expected_routes": [[], ["task5__t0"], ["task7__t0"]],
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
                {"task_id": "task5", "status": statuses.FAILED},
                {"task_id": "task7", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
            "expected_errors": [
                {
                    "message": "Execution failed. See result for details.",
                    "type": "error",
                    "task_id": "task3",
                },
                {
                    "message": "Execution failed. See result for details.",
                    "type": "error",
                    "task_id": "task5",
                },
                {
                    "message": "Execution failed. See result for details.",
                    "type": "error",
                    "task_id": "task7",
                },
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_context(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-context"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task6",
                "task8",
                "task3",
                "task5",
                "task7",
                "task9",
                "task10",
            ],
            "mock_action_executions": [
                {"task_id": "task2", "result": "Fee fi"},
                {"task_id": "task4", "result": "I smell the blood of an English man"},
                {"task_id": "task3", "result": "fo fum"},
                {"task_id": "task7", "result": "Be alive, or be he dead"},
            ],
            "expected_output": {
                "messages": [
                    "Fee fi fo fum",
                    "I smell the blood of an English man",
                    "Be alive, or be he dead",
                    "I'll grind his bones to make my bread",
                ]
            },
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_task_transition_on_completed_both_branches_succeeded(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-on-complete"),
            # The tasks before the join, task3 and task5, both succeeded.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_task_transition_on_completed_one_branch_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-on-complete"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
            # First tasks before the join, task3, failed.
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_task_transition_on_completed_other_branch_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-on-complete"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
            # Second tasks before the join, task5, failed.
            "mock_action_executions": [
                {"task_id": "task5", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_task_transition_on_completed_both_branches_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-on-complete"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
            # Both tasks before the join, task3 and task5, failed.
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
                {"task_id": "task5", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_task_transition_on_failed_both_branches_succeeded(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-on-fail"),
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_task_transition_on_failed_one_branch_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-on-fail"),
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5"],
            # First task before the join, task3, failed.
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
            "expected_errors": [
                {
                    "message": "Execution failed. See result for details.",
                    "type": "error",
                    "task_id": "task3",
                },
                {
                    "message": (
                        'UnreachableJoinError: The join task|route "task6|0" '
                        "is partially satisfied but unreachable."
                    ),
                    "type": "error",
                    "route": 0,
                    "task_id": "task6",
                },
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_task_transition_on_failed_other_branch_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-on-fail"),
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5"],
            # Second task before the join, task5, failed.
            "mock_action_executions": [
                {"task_id": "task5", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
            "expected_errors": [
                {
                    "message": "Execution failed. See result for details.",
                    "type": "error",
                    "task_id": "task5",
                },
                {
                    "message": (
                        'UnreachableJoinError: The join task|route "task6|0" '
                        "is partially satisfied but unreachable."
                    ),
                    "type": "error",
                    "route": 0,
                    "task_id": "task6",
                },
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_task_transition_on_failed_both_branches_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-on-fail"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
            # The tasks before the join, task3 and task5, both failed.
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
                {"task_id": "task5", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_all_complex_both_branches_succeeded(self):
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
        test_spec = {
            "workflow": self.get_wf_file_path("join-all-complex"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_all_complex_one_branch_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-all-complex"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
            # One of the branches failed and the other succeeded. Each
            # of the task will trigger different criteria in the task
            # transition but will still join on task6.
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_all_complex_other_branch_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-all-complex"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
            # One of the branches succeeded and the other failed. Each
            # of the task will trigger different criteria in the task
            # transition but will still join on task6.
            "mock_action_executions": [
                {"task_id": "task5", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_all_complex_both_branches_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-all-complex"),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
            # Both branch 1 and 2 failed and triggering a different
            # set of task transition but will still join on task6.
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
                {"task_id": "task5", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_complex_all_branches_succeeded(self):
        # In this workflow, the task task3, task5, and task7 join on task8.
        # There are multiple task transition with different criteria (succeeded,
        # failed, and completed) defined at each task. All the task transition
        # specify task8 as the next task. Previously, join count tallies any
        # inbound task transition to pass criteria. This is now changed such
        # that the count is on the inbound task and not on task transition.
        test_spec = {
            "workflow": self.get_wf_file_path("join-count-complex"),
            # All branches succeeded.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task6",
                "task3",
                "task5",
                "task7",
                "task8",
                "task9",
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_complex_one_branch_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count-complex"),
            # One of three branches failed. Branch 3 failed.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task6",
                "noop__r1",
                "task3",
                "task5",
                "task8",
                "task9",
            ],
            "expected_routes": [[], ["task6__t0"]],
            "mock_action_executions": [
                {"task_id": "task6", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_complex_other_branch_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count-complex"),
            # One of three branches failed. Branch 2 failed.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "noop__r1",
                "task6",
                "task3",
                "task7",
                "task8",
                "task9",
            ],
            "expected_routes": [[], ["task4__t0"]],
            "mock_action_executions": [
                {"task_id": "task4", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_complex_another_branch_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count-complex"),
            # One of three branches failed. Branch 1 failed.
            "expected_task_sequence": [
                "task1",
                "task2",
                "noop__r1",
                "task4",
                "task6",
                "task5",
                "task7",
                "task8",
                "task9",
            ],
            "expected_routes": [[], ["task2__t0"]],
            "mock_action_executions": [
                {"task_id": "task2", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_complex_two_branches_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count-complex"),
            # Two of three branches failed. Branch 2 and 3 failed.
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "noop__r1",
                "task6",
                "noop__r2",
                "task3",
            ],
            "expected_routes": [[], ["task4__t0"], ["task6__t0"]],
            "mock_action_executions": [
                {"task_id": "task4", "status": statuses.FAILED},
                {"task_id": "task6", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_complex_other_two_branches_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count-complex"),
            # Two of three branches failed. Branch 1 and 2 failed.
            "expected_task_sequence": [
                "task1",
                "task2",
                "noop__r1",
                "task4",
                "noop__r2",
                "task6",
                "task7",
            ],
            "expected_routes": [[], ["task2__t0"], ["task4__t0"]],
            "mock_action_executions": [
                {"task_id": "task2", "status": statuses.FAILED},
                {"task_id": "task4", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_join_count_complex_another_two_branches_failed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("join-count-complex"),
            # Two of three branches failed. Branch 1 and 3 failed.
            "expected_task_sequence": [
                "task1",
                "task2",
                "noop__r1",
                "task4",
                "task6",
                "noop__r2",
                "task5",
            ],
            "expected_routes": [[], ["task2__t0"], ["task6__t0"]],
            "mock_action_executions": [
                {"task_id": "task2", "status": statuses.FAILED},
                {"task_id": "task6", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()
