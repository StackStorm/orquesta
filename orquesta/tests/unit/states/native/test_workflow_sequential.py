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


class SequentialWorkflowStateTest(base.OrchestraWorkflowConductorTest):

    def __init__(self, *args, **kwargs):
        super(SequentialWorkflowStateTest, self).__init__(*args, **kwargs)
        self.wf_name = 'sequential'

    def assert_workflow_state(self, mock_flow_entries, expected_wf_states, conductor=None):
        return super(SequentialWorkflowStateTest, self).assert_workflow_state(
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
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
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
            {'id': 'task2', 'name': 'task2', 'state': states.FAILED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.FAILED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_pending(self):
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PENDING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSED
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Resume the workflow and assert the remaining states.
        conductor.request_workflow_state(states.RESUMING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.SUCCEEDED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_while_task_running(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.PAUSED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_and_resuming_while_task_running(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING}
        ]

        expected_wf_states = [
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
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_then_task_abended(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.FAILED}
        ]

        expected_wf_states = [
            states.FAILED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_then_task_canceled(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.CANCELED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_pausing_then_task_pending(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.PENDING}
        ]

        expected_wf_states = [
            states.PAUSED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_resuming(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.PAUSED
        ]

        # Assert the remaining states using the previous conductor.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor)

        # Resume the workflow and assert the remaining states.
        conductor.request_workflow_state(states.RESUMING)

        mock_flow_entries = [
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.SUCCEEDED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_resuming_then_task_abended(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.PAUSED
        ]

        # Assert the remaining states using the previous conductor.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor)

        # Resume the workflow and assert the remaining states.
        conductor.request_workflow_state(states.RESUMING)

        mock_flow_entries = [
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.FAILED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.FAILED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_workflow_resuming_then_task_canceled(self):
        # Run workflow until task2 is running.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING
        ]

        # Assert states and then save the conductor for later.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states)

        # Pause the workflow and assert the remaining states.
        conductor.request_workflow_state(states.PAUSING)

        mock_flow_entries = [
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.PAUSED
        ]

        # Assert the remaining states using the previous conductor.
        conductor = self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor)

        # Resume the workflow and assert the remaining states.
        conductor.request_workflow_state(states.RESUMING)

        mock_flow_entries = [
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.CANCELED
        ]

        # Assert the remaining states using the previous conductor.
        self.assert_workflow_state(mock_flow_entries, expected_wf_states, conductor=conductor)

    def test_task_pause(self):
        # Test use case where a task is paused.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_task_pausing_then_canceled(self):
        # Test use case where a task is pausing and then canceled.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_task_pausing_then_abended(self):
        # Test use case where a task is pausing and then failed.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.FAILED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.FAILED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_task_resume(self):
        # Test use case where a task is paused and resumed.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED},
            {'id': 'task2', 'name': 'task2', 'state': states.RESUMING},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED},
            {'id': 'task3', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task3', 'name': 'task3', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSED,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.SUCCEEDED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_task_resuming_then_canceled(self):
        # Test use case where a task is paused and resumed.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED},
            {'id': 'task2', 'name': 'task2', 'state': states.RESUMING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.PAUSED,
            states.RUNNING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_task_cancel(self):
        # Test use case where a task is canceled.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_task_canceling_then_paused(self):
        # Test use case where a task is canceling and then paused.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_task_canceling_then_abended(self):
        # Test use case where a task is pausing and then failed.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task2', 'name': 'task2', 'state': states.FAILED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)
