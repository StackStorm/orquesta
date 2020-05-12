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


class SequentialWorkflowStatusTest(base.OrchestraWorkflowConductorTest):
    def __init__(self, *args, **kwargs):
        super(SequentialWorkflowStatusTest, self).__init__(*args, **kwargs)
        self.wf_name = "sequential"

    def assert_workflow_status(self, mock_flow_entries, expected_wf_statuses, conductor=None):
        return super(SequentialWorkflowStatusTest, self).assert_workflow_status(
            self.wf_name, mock_flow_entries, expected_wf_statuses, conductor=conductor
        )

    def test_success(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.SUCCEEDED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_failure(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.FAILED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.FAILED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_pending(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PENDING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Resume the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.RESUMING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.SUCCEEDED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_while_task_running(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [{"id": "task2", "name": "task2", "status": statuses.SUCCEEDED}]

        expected_wf_statuses = [statuses.PAUSED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_and_resuming_while_task_running(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause and resume the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)
        conductor.request_workflow_status(statuses.RESUMING)

        mock_flow_entries = [{"id": "task2", "name": "task2", "status": statuses.SUCCEEDED}]

        expected_wf_statuses = [statuses.RUNNING]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_then_task_abended(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [{"id": "task2", "name": "task2", "status": statuses.FAILED}]

        expected_wf_statuses = [statuses.FAILED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_then_task_canceled(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [{"id": "task2", "name": "task2", "status": statuses.CANCELED}]

        expected_wf_statuses = [statuses.CANCELED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_then_task_pending(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [{"id": "task2", "name": "task2", "status": statuses.PENDING}]

        expected_wf_statuses = [statuses.PAUSED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_resuming(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [{"id": "task2", "name": "task2", "status": statuses.SUCCEEDED}]

        expected_wf_statuses = [statuses.PAUSED]

        # Assert the remaining statuses using the previous conductor.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor)

        # Resume the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.RESUMING)

        mock_flow_entries = [
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.SUCCEEDED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_resuming_then_task_abended(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [{"id": "task2", "name": "task2", "status": statuses.SUCCEEDED}]

        expected_wf_statuses = [statuses.PAUSED]

        # Assert the remaining statuses using the previous conductor.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor)

        # Resume the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.RESUMING)

        mock_flow_entries = [
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.FAILED},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.FAILED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_resuming_then_task_canceled(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.RUNNING]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [{"id": "task2", "name": "task2", "status": statuses.SUCCEEDED}]

        expected_wf_statuses = [statuses.PAUSED]

        # Assert the remaining statuses using the previous conductor.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor)

        # Resume the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.RESUMING)

        mock_flow_entries = [
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.CANCELED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_task_pause(self):
        # Test use case where a task is paused.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_task_pausing_then_canceled(self):
        # Test use case where a task is pausing and then canceled.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_task_pausing_then_abended(self):
        # Test use case where a task is pausing and then failed.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task2", "name": "task2", "status": statuses.FAILED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.FAILED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_task_resume(self):
        # Test use case where a task is paused and resumed.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
            {"id": "task2", "name": "task2", "status": statuses.RESUMING},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.SUCCEEDED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_task_resuming_then_canceled(self):
        # Test use case where a task is paused and resumed.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
            {"id": "task2", "name": "task2", "status": statuses.RESUMING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_task_cancel(self):
        # Test use case where a task is canceled.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_task_canceling_then_paused(self):
        # Test use case where a task is canceling and then paused.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_task_canceling_then_abended(self):
        # Test use case where a task is pausing and then failed.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task2", "name": "task2", "status": statuses.FAILED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)
