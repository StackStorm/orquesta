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


class WorkflowConductorContextTest(base.WorkflowConductorTest):

    def test_bad_app_ctx_references(self):
        wf_def = """
        version: 1.0

        input:
          - a: <% ctx().x %>

        vars:
          - b: <% ctx().y %>

        output:
          - x: <% ctx().a %>
          - y: <% ctx().b %>
          - z: <% ctx().z %>

        tasks:
          task1:
            action: core.noop
        """

        expected_errors = [
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to resolve key \'x\' in '
                    'expression \'<% ctx().x %>\' from context.'
                )
            },
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to resolve key \'y\' in '
                    'expression \'<% ctx().y %>\' from context.'
                )
            }
        ]

        spec = specs.WorkflowSpec(wf_def)

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Check workflow status and result.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertIsNone(conductor.get_workflow_output())

    def test_app_ctx_references(self):
        app_ctx = {
            'x': 'foobar',
            'y': 'fubar',
            'z': 'phobar'
        }

        wf_def = """
        version: 1.0

        input:
          - a: <% ctx().x %>

        vars:
          - b: <% ctx().y %>

        output:
          - x: <% ctx().a %>
          - y: <% ctx().b %>
          - z: <% ctx().z %>

        tasks:
          task1:
            action: core.noop
        """

        expected_output = app_ctx
        expected_errors = []

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(app_ctx=app_ctx), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec, context=app_ctx)
        conductor.request_workflow_state(states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.assertListEqual(conductor.errors, expected_errors)

        # Complete tasks
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # Check workflow status and output.
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
