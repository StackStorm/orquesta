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
from orquesta import exceptions as exc
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base


class WorkflowConductorContextTest(test_base.WorkflowConductorTest):
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

        # The YaqlEvaluationException on variables "x" and "y" are a result of using unassigned
        # variables in the input and vars section. The YaqlEvaluationException on variables
        # "a", "b", and "z" are a result of rendering workflow output with unassigned variables
        # on workflow execution completion.
        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to resolve key 'x' in "
                    "expression '<% ctx().x %>' from context."
                ),
            },
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to resolve key 'y' in "
                    "expression '<% ctx().y %>' from context."
                ),
            },
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to resolve key 'a' in "
                    "expression '<% ctx().a %>' from context."
                ),
            },
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to resolve key 'b' in "
                    "expression '<% ctx().b %>' from context."
                ),
            },
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to resolve key 'z' in "
                    "expression '<% ctx().z %>' from context."
                ),
            },
        ]

        spec = native_specs.WorkflowSpec(wf_def)

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec)

        self.assertRaises(
            exc.InvalidWorkflowStatusTransition, conductor.request_workflow_status, statuses.RUNNING
        )

        # Render workflow output and check workflow status and result.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertIsNone(conductor.get_workflow_output())

    def test_app_ctx_references(self):
        app_ctx = {"x": "foobar", "y": "fubar", "z": "phobar"}

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

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(app_ctx=app_ctx), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec, context=app_ctx)
        conductor.request_workflow_status(statuses.RUNNING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assertListEqual(conductor.errors, expected_errors)

        # Complete tasks
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_ctx_yaql_queries(self):
        wf_def = """
        version: 1.0

        input:
          - vms
          - vm1: <% ctx(vms).get(vm1) %>
          - vm2: <% ctx().vms.get(vm2) %>
          - vm3: <% ctx(vms)[vm3] %>
          - vm4: <% ctx().vms[vm4] %>

        vars:
          - vm1: <% ctx(vms).get(vm1) %>
          - vm2: <% ctx().vms.get(vm2) %>
          - vm3: <% ctx(vms)[vm3] %>
          - vm4: <% ctx().vms[vm4] %>

        tasks:
          task1:
            action: mock.create
            input:
              vm1: <% ctx(vms).get(vm1) %>
              vm2: <% ctx().vms.get(vm2) %>
              vm3: <% ctx(vms)[vm3] %>
              vm4: <% ctx().vms[vm4] %>
            next:
              - publish:
                  - vm1: <% ctx(vms).get(vm1) %>
                  - vm2: <% ctx().vms.get(vm2) %>
                  - vm3: <% ctx(vms)[vm3] %>
                  - vm4: <% ctx().vms[vm4] %>

        output:
          - vm1: <% ctx(vms).get(vm1) %>
          - vm2: <% ctx().vms.get(vm2) %>
          - vm3: <% ctx(vms)[vm3] %>
          - vm4: <% ctx().vms[vm4] %>
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

    def test_ctx_ref_private_var(self):
        app_ctx = {"__xyz": "phobar"}

        wf_def = """
        version: 1.0

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.noop
            next:
              - publish:
                  - task1: <% ctx("__xyz") %>

        output:
          - data: <% ctx("__xyz") %>
        """

        expected_inspection_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": '<% ctx("__xyz") %>',
                    "spec_path": "output[0]",
                    "schema_path": "properties.output",
                    "message": (
                        'Variable "__xyz" that is prefixed with double underscores '
                        "is considered a private variable and cannot be referenced."
                    ),
                },
                {
                    "type": "yaql",
                    "expression": '<% ctx("__xyz") %>',
                    "spec_path": "tasks.task1.next[0].publish[0]",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.next.items.properties.publish"
                    ),
                    "message": (
                        'Variable "__xyz" that is prefixed with double underscores '
                        "is considered a private variable and cannot be referenced."
                    ),
                },
            ]
        }

        expected_conducting_errors = [
            {
                "type": "error",
                "route": 0,
                "task_id": "task1",
                "task_transition_id": "continue__t0",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx(\"__xyz\") %>'. VariableInaccessibleError: The "
                    'variable "__xyz" is for internal use and inaccessible.'
                ),
            },
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx(\"__xyz\") %>'. VariableInaccessibleError: The "
                    'variable "__xyz" is for internal use and inaccessible.'
                ),
            },
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(app_ctx=app_ctx), expected_inspection_errors)

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec, context=app_ctx)
        conductor.request_workflow_status(statuses.RUNNING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Complete tasks
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_conducting_errors)
        self.assertIsNone(conductor.get_workflow_output())

    def test_ctx_get_all(self):
        app_ctx = {"__xyz": "phobar"}

        wf_def = """
        version: 1.0

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.noop
            next:
              - publish:
                  - task1: <% ctx() %>

        output:
          - data: <% ctx() %>
        """

        # Ensure the private variables prefixed with double underscores are not included.
        expected_output = {"data": {"foobar": "fubar", "task1": {"foobar": "fubar"}}}
        expected_errors = []

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(app_ctx=app_ctx), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec, context=app_ctx)
        conductor.request_workflow_status(statuses.RUNNING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Complete tasks
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
