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

from orchestra import exceptions as exc
from orchestra import states

from orchestra.tests.unit.conducting.native import base


class SequentialWorkflowStateTest(base.OrchestraWorkflowConductorTest):

    def __init__(self, *args, **kwargs):
        super(SequentialWorkflowStateTest, self).__init__(*args, **kwargs)
        self.wf_name = 'sequential'

    def assert_workflow_state(self, mock_flow_entries, expected_wf_states):
        return super(SequentialWorkflowStateTest, self).assert_workflow_state(
            self.wf_name,
            mock_flow_entries,
            expected_wf_states
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

    def test_pause(self):
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
            states.PAUSING,
            states.PAUSED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_pausing_then_canceled(self):
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
            states.PAUSING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_pausing_then_abended(self):
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
            states.PAUSING,
            states.PAUSED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_resume(self):
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
            states.PAUSING,
            states.PAUSED,
            states.RESUMING,
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.SUCCEEDED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_resuming_then_canceled(self):
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
            states.PAUSING,
            states.PAUSED,
            states.RESUMING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_cancel(self):
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

    def test_canceling_then_paused(self):
        # Test use case where a task is canceling and then paused.
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.CANCELING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSING},
            {'id': 'task2', 'name': 'task2', 'state': states.PAUSED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.CANCELING,
            states.CANCELING,
            states.CANCELED
        ]

        self.assertRaises(
            exc.InvalidStateTransition,
            self.assert_workflow_state,
            mock_flow_entries,
            expected_wf_states
        )

    def test_canceling_then_abended(self):
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
