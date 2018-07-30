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


class WorkflowErrorHandlingConductorTest(base.OrchestraWorkflowConductorTest):

    def test_error_log_fail(self):
        wf_name = 'error-log-fail'

        expected_task_seq = [
            'task1',
            'log',
            'fail'
        ]

        mock_states = [
            states.FAILED,      # task1
            states.SUCCEEDED    # log
        ]

        mock_results = [
            'All your base are belong to us!',  # task1
            'All your base are belong to us!'   # log
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states,
            mock_results=mock_results,
            expected_workflow_state=states.FAILED
        )

    def test_error_concurrent_log_fail(self):
        wf_name = 'error-log-fail-concurrent'

        expected_task_seq = [
            'task1',
            'fail'
        ]

        mock_states = [
            states.FAILED       # task1
        ]

        mock_results = [
            'All your base are belong to us!'   # task1
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_states=mock_states,
            mock_results=mock_results,
            expected_workflow_state=states.FAILED
        )
