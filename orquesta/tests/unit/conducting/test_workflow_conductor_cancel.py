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

from orquesta import conducting
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base


class WorkflowConductorCancelTest(test_base.WorkflowConductorTest):
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

        expected_output = {"x": 123, "y": 123}

        expected_errors = []

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow and keep it running.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])

        # Cancels the workflow and complete task1.
        conductor.request_workflow_status(statuses.CANCELING)
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)
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

        expected_output = {"x": 123, "y": 123}

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().y.value %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#value"'
                ),
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow and keep it running.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])

        # Cancels the workflow and complete task1.
        conductor.request_workflow_status(statuses.CANCELING)
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])

        # Check workflow status is not changed to failed given the output error.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_cancel_workflow_already_canceling(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow and keep it running.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])

        # Cancels the workflow and complete task1.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)

        # Cancels the workflow again.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)
        conductor.request_workflow_status(statuses.CANCELED)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)

        # Complete task1 and check workflow status.
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)
