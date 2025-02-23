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


class WorkflowErrorHandlingConductorTest(base.OrchestraWorkflowConductorTest):
    def test_error_log_fail(self):
        mock_result = "All your base are belong to us!"

        test_spec = {
            "workflow": self.get_wf_file_path("error-log-fail"),
            "expected_task_sequence": ["task1", "log", "fail"],
            "mock_action_executions": [
                {"task_id": "task1", "status": statuses.FAILED, "result": mock_result},
                {"task_id": "log", "result": mock_result},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_error_concurrent_log_fail(self):
        mock_result = "All your base are belong to us!"

        test_spec = {
            "workflow": self.get_wf_file_path("error-log-fail-concurrent"),
            "expected_task_sequence": ["task1", "fail", "log"],
            "mock_action_executions": [
                {"task_id": "task1", "status": statuses.FAILED, "result": mock_result},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_error_continue_success_path(self):
        test_spec = {
            "workflow": self.get_wf_file_path("error-handling-continue"),
            "expected_task_sequence": ["task1", "task2", "continue__r1"],
            "expected_routes": [
                [],  # default from start
                ["task2__t0"],  # task1 -> task2 -> continue
            ],
            "expected_output": {"message": "hooray!!!"},
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_error_continue_failure_path(self):
        test_spec = {
            "workflow": self.get_wf_file_path("error-handling-continue"),
            "expected_task_sequence": ["task1", "continue__r1"],
            "expected_routes": [
                [],  # default from start
                ["task1__t0"],  # task1 -> continue
            ],
            "mock_action_executions": [
                {"task_id": "task1", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
            "expected_output": {"message": "$%#&@#$!!!"},
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_error_noop_success_path(self):
        test_spec = {
            "workflow": self.get_wf_file_path("error-handling-noop"),
            "expected_task_sequence": ["task1", "task2", "continue"],
            "expected_output": {"message": "hooray!!!"},
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_error_noop_failure_path(self):
        test_spec = {
            "workflow": self.get_wf_file_path("error-handling-noop"),
            "expected_task_sequence": ["task1", "noop"],
            "mock_action_executions": [
                {"task_id": "task1", "status": statuses.FAILED},
            ],
            "expected_output": {"message": "$%#&@#$!!!"},
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_error_fail_success_path(self):
        test_spec = {
            "workflow": self.get_wf_file_path("error-handling-fail"),
            "expected_task_sequence": ["task1", "task2", "continue"],
            "expected_output": {"message": "hooray!!!"},
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_error_fail_failure_path(self):
        test_spec = {
            "workflow": self.get_wf_file_path("error-handling-fail"),
            "expected_task_sequence": ["task1", "task3", "fail"],
            "mock_action_executions": [
                {"task_id": "task1", "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
            "expected_output": {"message": "$%#&@#$!!!"},
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()
