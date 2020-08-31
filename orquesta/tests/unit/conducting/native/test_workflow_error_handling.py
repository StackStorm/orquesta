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
from orquesta.tests.mocks import WorkflowConductorMock
from orquesta.tests.unit.base import WorkflowComposerTest
from orquesta.tests.unit.conducting.native import base


class WorkflowErrorHandlingConductorTest(base.OrchestraWorkflowConductorTest, WorkflowComposerTest):
    def test_error_log_fail(self):
        wf_name = "error-log-fail"

        self.assert_spec_inspection(wf_name)

        expected_task_seq = ["task1", "log", "fail"]

        mock_statuses = [statuses.FAILED, statuses.SUCCEEDED]  # task1  # log

        mock_results = [
            "All your base are belong to us!",  # task1
            "All your base are belong to us!",  # log
        ]

        self.assert_spec_inspection(wf_name)

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            mock_statuses=mock_statuses,
            mock_results=mock_results,
            expected_workflow_status=statuses.FAILED,
        )
        # will throw
        mock.assert_conducting_sequences()

    def test_error_concurrent_log_fail(self):
        wf_name = "error-log-fail-concurrent"

        self.assert_spec_inspection(wf_name)

        expected_task_seq = ["task1", "fail", "log"]

        mock_statuses = [statuses.FAILED]  # task1

        mock_results = ["All your base are belong to us!"]  # task1

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            mock_statuses=mock_statuses,
            mock_results=mock_results,
            expected_workflow_status=statuses.FAILED,
        )
        # will throw
        mock.assert_conducting_sequences()

    def test_error_continue(self):
        wf_name = "error-handling-continue"

        self.assert_spec_inspection(wf_name)
        # Run thru the success path.
        expected_routes = [
            [],  # default from start
            ["task2__t0"],  # task1 -> task2 -> continue
        ]

        expected_task_seq = ["task1", "task2", ("continue", 1)]

        mock_statuses = [
            statuses.SUCCEEDED,  # task1
            statuses.SUCCEEDED,  # task2
            statuses.SUCCEEDED,  # continue
        ]

        expected_output = {"message": "hooray!!!"}

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=expected_routes,
            expected_output=expected_output,
        )
        # will throw
        mock.assert_conducting_sequences()

        # Run thru the failure path.
        expected_routes = [
            [],  # default from start
            ["task1__t0"],  # task1 -> continue
        ]

        expected_task_seq = ["task1", ("continue", 1)]

        mock_statuses = [statuses.FAILED, statuses.SUCCEEDED]  # task1  # continue

        expected_output = {"message": "$%#&@#$!!!"}

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=expected_routes,
            expected_workflow_status=statuses.FAILED,
            expected_output=expected_output,
        )
        # will throw
        mock.assert_conducting_sequences()

    def test_error_noop(self):
        wf_name = "error-handling-noop"

        self.assert_spec_inspection(wf_name)

        # Run thru the success path.
        expected_task_seq = ["task1", "task2", "continue"]

        mock_statuses = [
            statuses.SUCCEEDED,  # task1
            statuses.SUCCEEDED,  # task2
            statuses.SUCCEEDED,  # continue
        ]

        expected_output = {"message": "hooray!!!"}

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_output=expected_output,
        )
        # will throw
        mock.assert_conducting_sequences()

        # Run thru the failure path.
        expected_task_seq = ["task1", "noop"]

        mock_statuses = [statuses.FAILED, statuses.SUCCEEDED]  # task1  # noop

        expected_output = {"message": "$%#&@#$!!!"}

        mock = WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_output=expected_output,
        )
        # will throw
        mock.assert_conducting_sequences()

    def test_error_fail(self):
        wf_name = "error-handling-fail"

        self.assert_spec_inspection(wf_name)

        # Run thru the success path.
        expected_task_seq = ["task1", "task2", "continue"]

        mock_statuses = [
            statuses.SUCCEEDED,  # task1
            statuses.SUCCEEDED,  # task2
            statuses.SUCCEEDED,  # continue
        ]

        expected_output = {"message": "hooray!!!"}

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_output=expected_output,
        )
        # will throw
        mock.assert_conducting_sequences()

        # Run thru the failure path.
        expected_task_seq = ["task1", "task3", "fail"]

        mock_statuses = [
            statuses.FAILED,  # task1
            statuses.SUCCEEDED,  # task3
            statuses.FAILED,  # fail
        ]

        expected_output = {"message": "$%#&@#$!!!"}

        mock = WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_workflow_status=statuses.FAILED,
            expected_output=expected_output,
        )
        # will throw
        mock.assert_conducting_sequences()
