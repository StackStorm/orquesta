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


class WorkflowConductorContextTest(test_base.WorkflowConductorTest):
    def test_get_task_status_at_various_locations(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: task_status_at_publish=<% task_status(task1) %>
                do: task2
          task2:
            action: core.echo
            input:
              message: <% task_status(task1) %>

        output:
          - task_status_at_publish: <% ctx().task_status_at_publish %>
          - task_status_at_output: <% task_status(task1) %>
        """

        expected_errors = []
        expected_output = {
            "task_status_at_publish": "succeeded",
            "task_status_at_output": "succeeded",
        }

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assertListEqual(conductor.errors, expected_errors)

        # Complete task1.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Get next tasks and ensure task_status return expected result in task rendering.
        task2_ex_req = conductor.get_next_tasks()[0]
        self.assertEqual(task2_ex_req["id"], "task2")
        self.assertEqual(task2_ex_req["actions"][0]["action"], "core.echo")
        self.assertEqual(task2_ex_req["actions"][0]["input"]["message"], statuses.SUCCEEDED)

        # Complete task2.
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.SUCCEEDED])

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_get_task_status_of_tasks_along_split(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
            next:
              - do: task3
          task2:
            action: core.noop
            next:
              - do: task3
          task3:
            action: core.noop
            next:
              - publish:
                  - task1_status: <% task_status(task1) %>
                  - task2_status: <% task_status(task2) %>
                  - task3_status: <% task_status(task3) %>

        output:
          - task1_status: <% ctx(task1_status) %>
          - task2_status: <% ctx(task2_status) %>
          - task3_status: <% ctx(task3_status) %>
        """

        expected_errors = []
        expected_output = {
            "task1_status": "succeeded",
            "task2_status": "succeeded",
            "task3_status": "succeeded",
        }

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assertListEqual(conductor.errors, expected_errors)

        # Complete tasks
        status_changes = [statuses.RUNNING, statuses.SUCCEEDED]
        self.forward_task_statuses(conductor, "task1", status_changes)
        self.forward_task_statuses(conductor, "task2", status_changes)
        self.forward_task_statuses(conductor, "task3", status_changes, route=1)
        self.forward_task_statuses(conductor, "task3", status_changes, route=2)

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_get_task_status_of_tasks_along_splits(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task2
              - when: <% failed() %>
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
              - when: <% failed() %>
                do: task3
          task3:
            action: core.noop
            next:
              - publish:
                  - task1_status: <% task_status(task1) %>
                  - task2_status: <% task_status(task2) %>
                  - task3_status: <% task_status(task3) %>

        output:
          - task1_status: <% ctx(task1_status) %>
          - task2_status: <% ctx(task2_status) %>
          - task3_status: <% ctx(task3_status) %>
        """

        expected_errors = []
        expected_output = {
            "task1_status": "succeeded",
            "task2_status": "succeeded",
            "task3_status": "succeeded",
        }

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assertListEqual(conductor.errors, expected_errors)

        # Complete tasks
        status_changes = [statuses.RUNNING, statuses.SUCCEEDED]
        self.forward_task_statuses(conductor, "task1", status_changes)
        self.forward_task_statuses(conductor, "task2", status_changes, route=1)
        self.forward_task_statuses(conductor, "task3", status_changes, route=2)

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
