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


class ErrorHandlingWorkflowStatusTest(base.OrchestraWorkflowConductorTest):
    def __init__(self, *args, **kwargs):
        super(ErrorHandlingWorkflowStatusTest, self).__init__(*args, **kwargs)
        self.wf_name = "error-handling"

    def assert_workflow_status(self, mock_flow_entries, expected_wf_statuses, conductor=None):
        return super(ErrorHandlingWorkflowStatusTest, self).assert_workflow_status(
            self.wf_name, mock_flow_entries, expected_wf_statuses, conductor=conductor
        )

    def test_success(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.SUCCEEDED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_remediation(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.FAILED},
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.SUCCEEDED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_pausing_then_task_remediated(self):
        mock_flow_entries = [{"id": "task1", "name": "task1", "status": statuses.RUNNING}]

        expected_wf_statuses = [statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [{"id": "task1", "name": "task1", "status": statuses.FAILED}]

        expected_wf_statuses = [statuses.PAUSED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_and_resuming_then_task_remediated(self):
        mock_flow_entries = [{"id": "task1", "name": "task1", "status": statuses.RUNNING}]

        expected_wf_statuses = [statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause and resume the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)
        conductor.request_workflow_status(statuses.RESUMING)

        mock_flow_entries = [{"id": "task1", "name": "task1", "status": statuses.FAILED}]

        expected_wf_statuses = [statuses.RUNNING]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_canceling_then_task_remediated(self):
        mock_flow_entries = [{"id": "task1", "name": "task1", "status": statuses.RUNNING}]

        expected_wf_statuses = [statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Cancel the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.CANCELING)

        mock_flow_entries = [{"id": "task1", "name": "task1", "status": statuses.FAILED}]

        expected_wf_statuses = [statuses.CANCELED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)
