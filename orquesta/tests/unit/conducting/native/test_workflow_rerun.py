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


class WorkflowConductorRerunTest(base.OrchestraWorkflowConductorTest):
    def test_basic(self):
        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "inputs": {"name": "Stanley"},
            "expected_task_sequence": ["task1", "task2"],
            "mock_action_executions": [
                {"task_id": "task1", "result": "Stanley"},
                {"task_id": "task2", "status": statuses.FAILED},
            ],
            "expected_term_tasks": ["task2"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal1 = rehearsing.load_test_spec(test_spec)
        self.assertIsInstance(rehearsal1.session, rehearsing.WorkflowTestCase)
        rehearsal1.assert_conducting_sequence()

        test_spec = {
            "workflow_state": rehearsal1.conductor.serialize(),
            "rerun_tasks": [{"task_id": "task2"}],
            # Since task2 failed in the test above and rerun here, there are two instances of task2.
            "expected_task_sequence": ["task1", "task2", "task2", "task3", "continue"],
            "mock_action_executions": [
                {"task_id": "task2", "result": "All your base are belong to us!"},
                {"task_id": "task3", "result": "Stanley, All your base are belong to us!"},
            ],
            "expected_output": {"greeting": "Stanley, All your base are belong to us!"},
        }

        rehearsal2 = rehearsing.load_test_spec(test_spec)
        self.assertIsInstance(rehearsal2.session, rehearsing.WorkflowRerunTestCase)
        rehearsal2.assert_conducting_sequence()

    def test_fail_single_branch(self):
        # Initial run of workflow and failure at task2.
        test_spec = {
            "workflow": self.get_wf_file_path("parallel"),
            "expected_task_sequence": ["task1", "task4", "task2", "task5"],  # Fail task2.
            "mock_action_executions": [{"task_id": "task2", "status": statuses.FAILED}],
            "expected_term_tasks": ["task2", "task5"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal1 = rehearsing.load_test_spec(test_spec)
        rehearsal1.assert_conducting_sequence()

        # Rerun entire workflow and fail task2 again.
        test_spec = {
            "workflow_state": rehearsal1.conductor.serialize(),
            "expected_task_sequence": ["task1", "task4", "task2", "task5", "task2", "task6"],
            "mock_action_executions": [{"task_id": "task2", "status": statuses.FAILED}],
            "expected_term_tasks": ["task2", "task6"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal2 = rehearsing.load_test_spec(test_spec)
        rehearsal2.assert_conducting_sequence()

        # Rerun workflow from task2 only and complete the workflow.
        test_spec = {
            "workflow_state": rehearsal2.conductor.serialize(),
            "rerun_tasks": [{"task_id": "task2"}],
            "expected_task_sequence": [
                "task1",
                "task4",
                "task2",
                "task5",
                "task2",
                "task6",
                "task2",
                "task3",
            ],
            "expected_term_tasks": ["task3", "task6"],
        }

        rehearsal3 = rehearsing.load_test_spec(test_spec)
        rehearsal3.assert_conducting_sequence()

    def test_fail_multiple_branches(self):
        # Initial run of workflow and failure at task2 and task5.
        test_spec = {
            "workflow": self.get_wf_file_path("parallel"),
            "expected_task_sequence": ["task1", "task4", "task2", "task5"],  # Fail task2 and task5.
            "mock_action_executions": [
                {"task_id": "task2", "status": statuses.FAILED},
                {"task_id": "task5", "status": statuses.FAILED},
            ],
            "expected_term_tasks": ["task2", "task5"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal1 = rehearsing.load_test_spec(test_spec)
        rehearsal1.assert_conducting_sequence()

        # Rerun and complete workflow.
        test_spec = {
            "workflow_state": rehearsal1.conductor.serialize(),
            "expected_task_sequence": [
                "task1",
                "task4",
                "task2",
                "task5",
                "task2",
                "task5",
                "task3",
                "task6",
            ],
            "expected_term_tasks": ["task3", "task6"],
        }

        rehearsal2 = rehearsing.load_test_spec(test_spec)
        rehearsal2.assert_conducting_sequence()

    def test_fail_single_before_join(self):
        # Initial run of workflow.
        test_spec = {
            "workflow": self.get_wf_file_path("join"),
            # Fail task3 before join at task6.
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5"],
            "mock_action_executions": [{"task_id": "task3", "status": statuses.FAILED}],
            "expected_term_tasks": ["task3", "task5"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal1 = rehearsing.load_test_spec(test_spec)
        rehearsal1.assert_conducting_sequence()

        # Rerun and complete workflow.
        test_spec = {
            "workflow_state": rehearsal1.conductor.serialize(),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task3",
                "task6",
                "task7",
            ],
            "expected_term_tasks": ["task7"],
        }

        rehearsal2 = rehearsing.load_test_spec(test_spec)
        rehearsal2.assert_conducting_sequence()

    def test_fail_multiple_before_join(self):
        # Initial run of workflow.
        test_spec = {
            "workflow": self.get_wf_file_path("join"),
            # Fail task3 before join at task6.
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5"],
            "mock_action_executions": [
                {"task_id": "task3", "status": statuses.FAILED},
                {"task_id": "task5", "status": statuses.FAILED},
            ],
            "expected_term_tasks": ["task3", "task5"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal1 = rehearsing.load_test_spec(test_spec)
        rehearsal1.assert_conducting_sequence()

        # Rerun and complete workflow.
        test_spec = {
            "workflow_state": rehearsal1.conductor.serialize(),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task3",
                "task5",
                "task6",
                "task7",
            ],
            "expected_term_tasks": ["task7"],
        }

        rehearsal2 = rehearsing.load_test_spec(test_spec)
        rehearsal2.assert_conducting_sequence()

    def test_fail_at_join(self):
        # Initial run of workflow.
        test_spec = {
            "workflow": self.get_wf_file_path("join"),
            # Fail task3 before join at task6.
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5", "task6"],
            "mock_action_executions": [{"task_id": "task6", "status": statuses.FAILED}],
            "expected_term_tasks": ["task6"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal1 = rehearsing.load_test_spec(test_spec)
        rehearsal1.assert_conducting_sequence()

        # Rerun and complete workflow.
        test_spec = {
            "workflow_state": rehearsal1.conductor.serialize(),
            "expected_task_sequence": [
                "task1",
                "task2",
                "task4",
                "task3",
                "task5",
                "task6",
                "task6",
                "task7",
            ],
            "expected_term_tasks": ["task7"],
        }

        rehearsal2 = rehearsing.load_test_spec(test_spec)
        rehearsal2.assert_conducting_sequence()

    def test_fail_cycle(self):
        # Initial run of workflow.
        test_spec = {
            "workflow": self.get_wf_file_path("cycle"),
            # Fail task3 which is part of the cycle task1->task2->task3->task1.
            "expected_task_sequence": [
                "prep",
                "task1",
                "task2",
                "task3",
                "task1",
                "task2",
                "task3",
            ],
            "mock_action_executions": [
                {"task_id": "task3", "seq_id": 6, "status": statuses.FAILED},
            ],
            "expected_term_tasks": ["task3"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal1 = rehearsing.load_test_spec(test_spec)
        rehearsal1.assert_conducting_sequence()

        # Rerun and complete workflow.
        test_spec = {
            "workflow_state": rehearsal1.conductor.serialize(),
            "expected_task_sequence": [
                "prep",
                "task1",
                "task2",
                "task3",
                "task1",
                "task2",
                "task3",
                "task3",
                "task1",
                "task2",
                "task3",
            ],
            "expected_term_tasks": ["task3"],
        }

        rehearsal2 = rehearsing.load_test_spec(test_spec)
        rehearsal2.assert_conducting_sequence()

    def test_fail_at_single_split(self):
        # Initial run of workflow.
        test_spec = {
            "workflow": self.get_wf_file_path("split"),
            # Fail task5 at one of the split/fork.
            "expected_routes": [
                [],  # default from start
                ["task2__t0"],  # task1 -> task2 -> task4
                ["task3__t0"],  # task1 -> task3 -> task4
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r0",
                "task4__r1",
                "task4__r2",
                "task5__r1",
                "task5__r2",
                "task6__r1",
                "task6__r2",
            ],
            "mock_action_executions": [
                {"task_id": "task5", "seq_id": 6, "status": statuses.FAILED},
            ],
            "expected_term_tasks": ["task5__r2", "task6__r1", "task6__r2"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal1 = rehearsing.load_test_spec(test_spec)
        rehearsal1.assert_conducting_sequence()

        # Rerun and complete workflow.
        test_spec = {
            "workflow_state": rehearsal1.conductor.serialize(),
            "expected_routes": [
                [],  # default from start
                ["task2__t0"],  # task1 -> task2 -> task4
                ["task3__t0"],  # task1 -> task3 -> task4
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r0",
                "task4__r1",
                "task4__r2",
                "task5__r1",
                "task5__r2",
                "task6__r1",
                "task6__r2",
                "task5__r2",
                "task7__r1",
                "task7__r2",
            ],
            "expected_term_tasks": ["task7__r1", "task7__r2"],
        }

        rehearsal2 = rehearsing.load_test_spec(test_spec)
        rehearsal2.assert_conducting_sequence()

    def test_fail_at_multiple_splits(self):
        # Initial run of workflow.
        test_spec = {
            "workflow": self.get_wf_file_path("split"),
            # Fail task5 at one of the split/fork and task6 in another split/fork.
            "expected_routes": [
                [],  # default from start
                ["task2__t0"],  # task1 -> task2 -> task4
                ["task3__t0"],  # task1 -> task3 -> task4
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r0",
                "task4__r1",
                "task4__r2",
                "task5__r1",
                "task5__r2",
                "task6__r1",
                "task6__r2",
            ],
            "mock_action_executions": [
                {"task_id": "task5", "seq_id": 6, "status": statuses.FAILED},
                {"task_id": "task6", "seq_id": 7, "status": statuses.FAILED},
            ],
            "expected_term_tasks": ["task5__r2", "task6__r1", "task6__r2"],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal1 = rehearsing.load_test_spec(test_spec)
        rehearsal1.assert_conducting_sequence()

        # Rerun and complete workflow.
        test_spec = {
            "workflow_state": rehearsal1.conductor.serialize(),
            "expected_routes": [
                [],  # default from start
                ["task2__t0"],  # task1 -> task2 -> task4
                ["task3__t0"],  # task1 -> task3 -> task4
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r0",
                "task4__r1",
                "task4__r2",
                "task5__r1",
                "task5__r2",
                "task6__r1",
                "task6__r2",
                "task5__r2",
                "task6__r1",
                "task7__r1",
                "task7__r2",
            ],
            "expected_term_tasks": ["task7__r1", "task7__r2"],
        }

        rehearsal2 = rehearsing.load_test_spec(test_spec)
        rehearsal2.assert_conducting_sequence()
