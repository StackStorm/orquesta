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


class BranchingWorkflowStatusTest(base.OrchestraWorkflowConductorTest):
    def __init__(self, *args, **kwargs):
        super(BranchingWorkflowStatusTest, self).__init__(*args, **kwargs)
        self.wf_name = "branching"

    def assert_workflow_status(self, mock_flow_entries, expected_wf_statuses, conductor=None):
        return super(BranchingWorkflowStatusTest, self).assert_workflow_status(
            self.wf_name, mock_flow_entries, expected_wf_statuses, conductor=conductor
        )

    def test_success(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task5", "name": "task5", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.SUCCEEDED},
            {"id": "task5", "name": "task5", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
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
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.FAILED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.FAILED,
            statuses.FAILED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_pending(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PENDING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSING,
            statuses.PAUSED,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Resume the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.RESUMING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task5", "name": "task5", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.SUCCEEDED},
            {"id": "task5", "name": "task5", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.SUCCEEDED,
        ]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_multiple_pendings(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PENDING},
            {"id": "task4", "name": "task4", "status": statuses.PENDING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSING,
            statuses.PAUSED,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Resolve the pending tasks.
        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [statuses.PAUSED, statuses.PAUSED]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

        # Resume the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.RESUMING)

        mock_flow_entries = [
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task5", "name": "task5", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.SUCCEEDED},
            {"id": "task5", "name": "task5", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.SUCCEEDED,
        ]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_then_branch1_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [statuses.PAUSING, statuses.PAUSED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_and_resuming_then_branch1_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause and resume the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)
        conductor.request_workflow_status(statuses.RESUMING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [statuses.RUNNING, statuses.RUNNING]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_then_branch1_pausing_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
        ]

        expected_wf_statuses = [statuses.PAUSING, statuses.PAUSING, statuses.PAUSED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_then_branch1_canceling_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [statuses.CANCELING, statuses.CANCELING, statuses.CANCELED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_pausing_then_branch1_abended_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Pause the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.PAUSING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.FAILED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [statuses.FAILED, statuses.FAILED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_canceling_then_branch1_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Cancel the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.CANCELING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [statuses.CANCELING, statuses.CANCELED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_canceling_then_branch1_pausing_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Cancel the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.CANCELING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
        ]

        expected_wf_statuses = [statuses.CANCELING, statuses.CANCELING, statuses.CANCELED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_canceling_then_branch1_canceling_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Cancel the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.CANCELING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [statuses.CANCELING, statuses.CANCELING, statuses.CANCELED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_canceling_then_branch1_abended_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        # Assert statuses and then save the conductor for later.
        conductor = self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

        # Cancel the workflow and assert the remaining statuses.
        conductor.request_workflow_status(statuses.CANCELING)

        mock_flow_entries = [
            {"id": "task2", "name": "task2", "status": statuses.FAILED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [statuses.CANCELING, statuses.CANCELED]

        # Assert the remaining statuses using the previous conductor.
        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses, conductor=conductor)

    def test_workflow_running_then_branch1_pausing_and_branch2_succeeded(self):
        # Test use case where a task is still pausing while another task succeeded.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSING,
            statuses.PAUSED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_pausing_and_branch2_abended(self):
        # Test use case where a task is still pausing while another task abended.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.FAILED},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.FAILED,
            statuses.FAILED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_pausing_and_branch2_canceled(self):
        # Test use case where a task is still pausing while another task canceled.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.CANCELING},
            {"id": "task4", "name": "task4", "status": statuses.CANCELED},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_paused_and_branch2_succeeded(self):
        # Test use case where a task is paused and then another task succeeded.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_pending_and_branch2_succeeded(self):
        # Test use case where a task is paused and then another task succeeded.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PENDING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSING,
            statuses.PAUSED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_paused_and_branch2_abended(self):
        # Test use case where a task is paused and then another task abended.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
            {"id": "task4", "name": "task4", "status": statuses.FAILED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.FAILED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_paused_and_branch2_canceled(self):
        # Test use case where a task is paused and then another task canceled.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSING},
            {"id": "task2", "name": "task2", "status": statuses.PAUSED},
            {"id": "task4", "name": "task4", "status": statuses.CANCELING},
            {"id": "task4", "name": "task4", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch_paused_and_resuming(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.SUCCEEDED},
            {"id": "task4", "name": "task4", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.PAUSED},
            {"id": "task4", "name": "task4", "status": statuses.RESUMING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task3", "name": "task3", "status": statuses.RUNNING},
            {"id": "task5", "name": "task5", "status": statuses.RUNNING},
            {"id": "task3", "name": "task3", "status": statuses.SUCCEEDED},
            {"id": "task5", "name": "task5", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.SUCCEEDED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_paused_and_resuming_while_branch2_running(self):
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.PAUSED},
            {"id": "task4", "name": "task4", "status": statuses.RESUMING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_paused_and_resuming_while_branch2_canceling(self):
        # Test use case where a task is still canceling while another task paused.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task4", "name": "task4", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.PAUSED},
            {"id": "task4", "name": "task4", "status": statuses.RESUMING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_canceling_and_branch2_succeeded(self):
        # Test use case where a task is still canceling while another task succeeded.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_canceling_and_branch2_abended(self):
        # Test use case where a task is still canceling while another task abended.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task4", "name": "task4", "status": statuses.FAILED},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_canceling_and_branch2_paused(self):
        # Test use case where a task is still canceling while another task paused.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task4", "name": "task4", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.PAUSED},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_canceled_and_branch2_succeeded(self):
        # Test use case where a task is canceled and then another task succeeded.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
            {"id": "task4", "name": "task4", "status": statuses.SUCCEEDED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_canceled_and_branch2_abended(self):
        # Test use case where a task is canceled and then another task abended.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
            {"id": "task4", "name": "task4", "status": statuses.FAILED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)

    def test_workflow_running_then_branch1_canceled_and_branch2_paused(self):
        # Test use case where a task is canceled and then another task paused.
        mock_flow_entries = [
            {"id": "task1", "name": "task1", "status": statuses.RUNNING},
            {"id": "task1", "name": "task1", "status": statuses.SUCCEEDED},
            {"id": "task2", "name": "task2", "status": statuses.RUNNING},
            {"id": "task4", "name": "task4", "status": statuses.RUNNING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELING},
            {"id": "task2", "name": "task2", "status": statuses.CANCELED},
            {"id": "task4", "name": "task4", "status": statuses.PAUSING},
            {"id": "task4", "name": "task4", "status": statuses.PAUSED},
        ]

        expected_wf_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED,
        ]

        self.assert_workflow_status(mock_flow_entries, expected_wf_statuses)
