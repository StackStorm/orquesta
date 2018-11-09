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

from orquesta import conducting
from orquesta import events
from orquesta.specs import native as specs
from orquesta import states
from orquesta.tests.unit import base


class WorkflowConductorCancelTest(base.WorkflowConductorTest):

    def test_workflow_output(self):
        wf_def = """
        version: 1.0

        output:
          - x: 123
          - y: <% ctx().x %>

        tasks:
          task1:
            action: core.noop
        """

        expected_output = {
            'x': 123,
            'y': 123
        }

        expected_errors = []

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow and keep it running.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))

        # Cancels the workflow and complete task1.
        conductor.request_workflow_state(states.CANCELING)
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # Check workflow status and output.
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_workflow_output_with_error(self):
        wf_def = """
        version: 1.0

        output:
          - x: 123
          - y: <% ctx().x %>
          - z: <% ctx().y.value %>

        tasks:
          task1:
            action: core.noop
        """

        expected_output = {
            'x': 123,
            'y': 123
        }

        expected_errors = [
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().y.value %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#value"'
                )
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow and keep it running.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))

        # Cancels the workflow and complete task1.
        conductor.request_workflow_state(states.CANCELING)
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # Check workflow status is not changed to failed given the output error.
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
