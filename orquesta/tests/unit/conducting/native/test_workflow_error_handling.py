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
        wf_name = "error-log-fail"

        expected_task_seq = ["task1", "log", "fail"]

        mock_result = "All your base are belong to us!"

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", status=statuses.FAILED, result=mock_result),
            rehearsing.MockActionExecution("log", result=mock_result),
        ]

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_error_concurrent_log_fail(self):
        wf_name = "error-log-fail-concurrent"

        expected_task_seq = ["task1", "fail", "log"]

        mock_result = "All your base are belong to us!"

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", status=statuses.FAILED, result=mock_result),
        ]

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_error_continue_success_path(self):
        wf_name = "error-handling-continue"

        expected_routes = [
            [],  # default from start
            ["task2__t0"],  # task1 -> task2 -> continue
        ]

        expected_task_seq = ["task1", "task2", ("continue", 1)]

        expected_output = {"message": "hooray!!!"}

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            expected_routes=expected_routes,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_error_continue_failure_path(self):
        wf_name = "error-handling-continue"

        expected_routes = [
            [],  # default from start
            ["task1__t0"],  # task1 -> continue
        ]

        expected_task_seq = ["task1", ("continue", 1)]

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", status=statuses.FAILED),
        ]

        expected_output = {"message": "$%#&@#$!!!"}

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_routes=expected_routes,
            expected_workflow_status=statuses.FAILED,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_error_noop_success_path(self):
        wf_name = "error-handling-noop"

        expected_task_seq = ["task1", "task2", "continue"]

        expected_output = {"message": "hooray!!!"}

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_error_noop_failure_path(self):
        wf_name = "error-handling-noop"

        expected_task_seq = ["task1", "noop"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", status=statuses.FAILED),
        ]

        expected_output = {"message": "$%#&@#$!!!"}

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_error_fail_success_path(self):
        wf_name = "error-handling-fail"

        expected_task_seq = ["task1", "task2", "continue"]

        expected_output = {"message": "hooray!!!"}

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_error_fail_failure_path(self):
        wf_name = "error-handling-fail"

        expected_task_seq = ["task1", "task3", "fail"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", status=statuses.FAILED),
        ]

        expected_output = {"message": "$%#&@#$!!!"}

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_output=expected_output,
            expected_workflow_status=statuses.FAILED,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()
