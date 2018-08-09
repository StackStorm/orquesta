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

from orquesta import states

from orquesta.tests.unit.conducting.native import base


class BranchingWorkflowStateTest(base.OrchestraWorkflowConductorTest):

    def __init__(self, *args, **kwargs):
        super(BranchingWorkflowStateTest, self).__init__(*args, **kwargs)
        self.wf_name = 'branching'

    def assert_workflow_state(self, mock_flow_entries, expected_wf_states, conductor=None):
        return super(BranchingWorkflowStateTest, self).assert_workflow_state(
            self.wf_name,
            mock_flow_entries,
            expected_wf_states,
            conductor=conductor
        )

    def test_success(self):
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task5', 'name': 'task5', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.SUCCEEDED},
            {'id': 'task5', 'name': 'task5', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.SUCCEEDED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_failure(self):
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.FAILED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.FAILED,
            states.FAILED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_pending(self):
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PENDING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSING,
            states.PAUSED
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Resume the workflow and assert the remaining states.
        conductor.request_workflow_state(states.RESUMING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task5', 'name': 'task5', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.SUCCEEDED},
            {'id': 'task5', 'name': 'task5', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.SUCCEEDED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_multiple_pendings(self):
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PENDING},
            {'id': 'task4', 'name': 'task4', 'state': states.PENDING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSING,
            states.PAUSED
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Resolve the pending tasks.
        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.PAUSED,
            states.PAUSED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

        # Resume the workflow and assert the remaining states.
        conductor.request_workflow_state(states.RESUMING)

        mock_flow_entries = [
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task5', 'name': 'task5', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.SUCCEEDED},
            {'id': 'task5', 'name': 'task5', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.SUCCEEDED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_then_branch1_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.PAUSING,
            states.PAUSED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_and_resuming_then_branch1_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause and resume the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)
        conductor.request_workflow_state(states.RESUMING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_then_branch1_pausing_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED}
        ]

        expected_wf_states = [
            states.PAUSING,
            states.PAUSING,
            states.PAUSED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_then_branch1_canceling_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_then_branch1_abended_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.FAILED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.FAILED,
            states.FAILED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_canceling_then_branch1_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Cancel the workflow and assert the remaining states.
        conductor.request_workflow_state(states.CANCELING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.CANCELING,
            states.CANCELED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_canceling_then_branch1_pausing_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Cancel the workflow and assert the remaining states.
        conductor.request_workflow_state(states.CANCELING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED}
        ]

        expected_wf_states = [
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_canceling_then_branch1_canceling_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Cancel the workflow and assert the remaining states.
        conductor.request_workflow_state(states.CANCELING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_canceling_then_branch1_abended_and_branch2_succeeded(self):
        # Run workflow until both branches are running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Cancel the workflow and assert the remaining states.
        conductor.request_workflow_state(states.CANCELING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.FAILED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.CANCELING,
            states.CANCELED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_running_then_branch1_pausing_and_branch2_succeeded(self):
        # Test use case where a task is still pausing while another task succeeded.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED},
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSING,
            states.PAUSED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_pausing_and_branch2_abended(self):
        # Test use case where a task is still pausing while another task abended.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.FAILED},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED},
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.FAILED,
            states.FAILED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_pausing_and_branch2_canceled(self):
        # Test use case where a task is still pausing while another task canceled.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.CANCELING},
            {'id': 'task4', 'name': 'task4', 'state': states.CANCELED},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED},
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_paused_and_branch2_succeeded(self):
        # Test use case where a task is paused and then another task succeeded.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_pending_and_branch2_succeeded(self):
        # Test use case where a task is paused and then another task succeeded.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PENDING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSING,
            states.PAUSED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_paused_and_branch2_abended(self):
        # Test use case where a task is paused and then another task abended.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED},
            {'id': 'task4', 'name': 'task4', 'state': states.FAILED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.FAILED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_paused_and_branch2_canceled(self):
        # Test use case where a task is paused and then another task canceled.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED},
            {'id': 'task4', 'name': 'task4', 'state': states.CANCELING},
            {'id': 'task4', 'name': 'task4', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch_paused_and_resuming(self):
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSED},
            {'id': 'task4', 'name': 'task4', 'state': states.RESUMING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task5', 'name': 'task5', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.SUCCEEDED},
            {'id': 'task5', 'name': 'task5', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSED,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.SUCCEEDED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_paused_and_resuming_while_branch2_running(self):
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSED},
            {'id': 'task4', 'name': 'task4', 'state': states.RESUMING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_paused_and_resuming_while_branch2_canceling(self):
        # Test use case where a task is still canceling while another task paused.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSED},
            {'id': 'task4', 'name': 'task4', 'state': states.RESUMING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_canceling_and_branch2_succeeded(self):
        # Test use case where a task is still canceling while another task succeeded.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_canceling_and_branch2_abended(self):
        # Test use case where a task is still canceling while another task abended.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task4', 'name': 'task4', 'state': states.FAILED},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_canceling_and_branch2_paused(self):
        # Test use case where a task is still canceling while another task paused.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSED},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_canceled_and_branch2_succeeded(self):
        # Test use case where a task is canceled and then another task succeeded.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED},
            {'id': 'task4', 'name': 'task4', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_canceled_and_branch2_abended(self):
        # Test use case where a task is canceled and then another task abended.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED},
            {'id': 'task4', 'name': 'task4', 'state': states.FAILED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_workflow_running_then_branch1_canceled_and_branch2_paused(self):
        # Test use case where a task is canceled and then another task paused.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task4', 'name': 'task4', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSING},
            {'id': 'task4', 'name': 'task4', 'state': states.PAUSED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)
