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

from orchestra import states

from orchestra.tests.unit.conducting.native import base


class ErrorHandlingWorkflowStateTest(base.OrchestraWorkflowConductorTest):

    def __init__(self, *args, **kwargs):
        super(ErrorHandlingWorkflowStateTest, self).__init__(*args, **kwargs)
        self.wf_name = 'error-handling'

    def assert_workflow_state(self, mock_flow_entries, expected_wf_states):
        return super(ErrorHandlingWorkflowStateTest, self).assert_workflow_state(
            self.wf_name,
            mock_flow_entries,
            expected_wf_states
        )

    def test_success(self):
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.SUCCEEDED},
            {'id': 'task2', 'name': 'task2', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task2', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.SUCCEEDED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)

    def test_failure(self):
        mock_flow_entries = [
            {'id': 'task1', 'name': 'task1', 'state': states.RUNNING},
            {'id': 'task1', 'name': 'task1', 'state': states.FAILED},
            {'id': 'task2', 'name': 'task3', 'state': states.RUNNING},
            {'id': 'task2', 'name': 'task3', 'state': states.SUCCEEDED}
        ]

        expected_wf_states = [
            states.RUNNING,
            states.RUNNING,
            states.RUNNING,
            states.SUCCEEDED
        ]

        self.assert_workflow_state(mock_flow_entries, expected_wf_states)
